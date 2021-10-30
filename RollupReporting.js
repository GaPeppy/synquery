
const assert = require('assert')
const GCredArray = $secure.MASTER_ACCOUNT_CONTEXT_USER_KEY_CSV.split(',')
const GBatchID = Date.now()

//
// function definitions
//
////
// NerdGraph() -> general purpose NerdGraph wrapper
////
function NerdGraph(sngcred,squery,svar){
  return new Promise((resolve, reject) => {
    const options = {url: 'https://api.newrelic.com/graphql', gzip:true, body:{query:squery,variables:svar}, json:true, headers:{'Api-key':sngcred}}
    //logit('NerdGraph','options',options)
    $http.post(options, (err,response,body) => {
      if (err) {
        return reject(err)
      }
      if (response.statusCode < 200 || response.statusCode > 299) {
        return reject('Failed to load page, status code: ' + response.statusCode);
      }
      if(body.errors !== undefined){
        logit('NerdGraph','body.errors',squery, svar, sngcred.substring(0,10),body.errors)
        return reject('NerdGraph failed')
      }
      return resolve(body)
    })
  })
}
function CaptureCustomAttribute(sMCred,sTag,sValue){
  $util.insights.set('MasterCred.' + sTag + '.' + sMCred.substring(0,10), sValue)
}
//
// PostEvents() -> Push results into custom Event Table
//
function PostEvents(sCred,sTargetAcctId,sTable,ntimestamp,aEvents){
  return new Promise((resolve, reject) => {
    if(!Array.isArray(aEvents) || aEvents.length == 0){var response = {statusCode:599}; return resolve(response)}
    logit('PostEvents','about to create number of events',aEvents.length,sTable)
    for (oa of aEvents){
      oa.eventType = sTable
      oa.timestamp = ntimestamp
    }
    const options = {url: `https://insights-collector.newrelic.com/v1/accounts/${sTargetAcctId}/events`, gzip:true, body:aEvents, json:true, headers:{'Api-key':sCred}}
    //debug line
    //logit('PostEvents','options',options)
    $http.post(options, (err,response,body) => {
      if (err) {
        return reject(err)
      }
      if (response.statusCode < 200 || response.statusCode > 299) {
        return reject('Failed to load page, status code: ' + response.statusCode);
      }
      return resolve(response)
    })
  })
}
//
// GetAccounts() -> Get list of subaccounts for the current Master Context
//{ data: { actor: { accounts: [Array] } } }
function GetAccounts(sCred){
  return new Promise((resolve, reject) => {
    const CQuery = `{
      actor {
        accounts(scope: IN_REGION) {
          id
          name
        }
      }
    }`
    //logit('GetAccounts','creds',sCred.substring(0,10)+'****')
    NerdGraph(sCred,CQuery,'').then(payload => {
      var aAccounts = payload.data.actor.accounts
      CaptureCustomAttribute(sCred,'Accounts.Count',aAccounts.length)
      return resolve(aAccounts)
    })
  })
}
//
// GetCounts() -> run parallel queries for a target subaccount
//
function GetCounts(sCred,oAcct){
    const CQuery = `
{
  actor {
    account(id: RPMID) {
      lambdauc: nrql(query: "SELECT uniqueCount(entityGuid) as uc FROM AwsLambdaInvocation WHERE provider = 'LambdaFunction' SINCE 1 day ago") {
        results
      }
      ec2uc: nrql(query: "SELECT uniqueCount(entityGuid) as uc FROM ComputeSample WHERE provider = 'Ec2Instance' SINCE 1 day ago") {
        results
      }
      cloud {
        linkedAccounts {
          createdAt
          disabled
          externalId
          id
          metricCollectionMode
          name
          nrAccountId
          updatedAt
          integrations {
            service {
              isEnabled
              updatedAt
              createdAt
            }
            id
            name
          }
          provider {
            slug
          }
        }
      }
    }
  }
}`

  return NerdGraph(sCred,CQuery.replace(/RPMID/g,oAcct.id),'').then((payload)=>{
    return new Promise((resolve,reject)=>{
      try{
        var cres = {}
        cres.ucresult = {NrAccountId:oAcct.id, NrAccountName:oAcct.name, ReportingPeriod:'1d', BatchId:GBatchID.toString()}
        cres.ucresult.LambdaServerlessUC = payload.data.actor.account.lambdauc.results[0].uc
        cres.ucresult.EC2HostUC = payload.data.actor.account.ec2uc.results[0].uc
        cres.ucresult["CloudIntegrations.LinkedAccounts.Count"] = payload.data.actor.account.cloud.linkedAccounts.length
        var nServices = 0
        payload.data.actor.account.cloud.linkedAccounts.forEach( el => {
          if(el.disabled == false){
            el.integrations.forEach( iel => {
              if(iel.service.isEnabled){
                nServices++
              }
            })
          }
        })
        cres.ucresult["CloudIntegrations.Services.Count"] = nServices

        return resolve(cres)
      }
      catch (e) {
        logit('GetCounts','caught exception',oAcct,e,payload)
        return reject(e)
      }
    })
  })
}
//
// LoopAccounts() -> scatter and gather for current MasterContext; parallel execution
//
function LoopAccounts(sCred, aAccounts){
  parray = []
  for(oa of aAccounts){
    parray.push(GetCounts(sCred,oa))
  }
  return Promise.all(parray)
}
//
// currently not used
//
function LoopAccountsSerial(sCred, aAccounts){
  return aAccounts.reduce(function(pr, oa) {
    return pr.then(function() {
      var ucresult = {NrAccountId:oa.id, NrAccountName:oa.name, ReportingPeriod:'1d', BatchId:GBatchID}
      return GetCounts(sCred,oa.id,ucresult)
    })
  }, Promise.resolve());
}
//
//stdout logger
//
function logit(mname,msg, ...theargs){
  if(mname == null || msg == null){throw('logit(method,msg,...) requires at least 2 params')}
  console.log(`[${(new Date()).toISOString()}]${mname}()-> ${msg}${theargs.length == 0 ? '' : ':'}`,...theargs)
}
//
//
//
function ProcessMasterContext(sMasterCred, nBatchID){
  return GetAccounts(sMasterCred)
  .then((aAccounts) =>{
    //console.log('GetAccounts() result:',payload)
    logit('ProcessMasterContext','working on number of accounts',aAccounts.length)
    //logit('ProcessMasterContext','aAccounts',aAccounts)
    return LoopAccounts(sMasterCred, aAccounts)
  }).then((aCounts)=>{
    //
    // The return structure is an array of response objects that could contain multiple objects in the future
    //
    var uCounts = []
    aCounts.forEach(el => {uCounts.push(el.ucresult)})
    //
    // push array of results in thru Events API
    return PostEvents($secure.POA_INGEST_KEY,$secure.POA_ACCOUNT_ID,"NrEntityRollupUsage",nBatchID,uCounts)
    .then((response1)=>{
      logit('ProcessMasterContext','postevents-response1',response1.statusCode)
    })
  }).catch(err => {
    logit('ProcessMasterContext','caught error on promise-chain',err)
    assert.fail(err)
  })
}

//force serial processing of Master-Context
logit('main','starting the run')
var sTargetTable = 'NrEntityRollupUsage'
$util.insights.set('BatchId',GBatchID.toString())
$util.insights.set('EntityTargetTable','NrEntityRollupUsage')
ProcessMasterContext(GCredArray[0],GBatchID)
.then(()=>{
  logit('main','stopwatch 0 in secs',(Date.now()-GBatchID)/1000)
  return ProcessMasterContext(GCredArray[1],GBatchID)
}).then(()=>{
  logit('main','stopwatch 1 in secs',(Date.now()-GBatchID)/1000)
  return ProcessMasterContext(GCredArray[2],GBatchID)
}).then(()=>{
  logit('main','stopwatch 2 in secs',(Date.now()-GBatchID)/1000)
})
