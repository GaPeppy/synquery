const GBatchID = Date.now()
const assert = require('assert')
const GCredArray = $secure.MASTER_ACCOUNT_CONTEXT_USER_KEY_CSV.split(',')
const GPOA_ACCOUNT_ID = $secure.POA_ACCOUNT_ID
const GPOA_INGEST_KEY = $secure.POA_INGEST_KEY
const GRollupTable = 'NrEntityRollupUsage'

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
function GetCounts(sCred,oAcct,nBatchID){
    const CQuery = `
{
  actor {
    dbcount: entitySearch(query: "type = 'DASHBOARD' and accountId = 'RPMID'") {
      count
    }
    iacount: entitySearch(query: "type = 'HOST' and accountId = 'RPMID'") {
      count
    }
    account(id: RPMID) {
      lambdauc: nrql(query: "SELECT uniqueCount(entityGuid) as uc FROM AwsLambdaInvocation WHERE provider = 'LambdaFunction' SINCE 1 day ago") {
        results
      }
      lambdaciuc: nrql(query: "SELECT uniqueCount(entityGuid) as uc from ServerlessSample where provider = 'LambdaFunction' SINCE 1 day ago") {
        results
      }
      ec2uc: nrql(query: "SELECT uniqueCount(entityGuid) as uc FROM ComputeSample WHERE provider = 'Ec2Instance' SINCE 1 day ago") {
        results
      }
      aiiopenuc: nrql(query: "SELECT uniqueCount(incidentId) as uc from NrAiIncident where event = 'open' since 1 day ago") {
        results
      }
      aiicloseuc: nrql(query: "SELECT uniqueCount(incidentId) as uc from NrAiIncident where event = 'close' since 1 day ago") {
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
        cres.ucresult = {NrAccountId:oAcct.id, NrAccountName:oAcct.name, ReportingPeriod:'1d', BatchId:nBatchID.toString()}
        cres.ucresult["Dashboard.Count"] = payload.data.actor.dbcount.count
        cres.ucresult["InfraAgent.Count"] = payload.data.actor.iacount.count
        cres.ucresult["Serverless.Lambda.UCount"] = payload.data.actor.account.lambdauc.results[0].uc
        cres.ucresult["Cloudwatch.Lambda.UCount"] = payload.data.actor.account.lambdaciuc.results[0].uc
        cres.ucresult["Cloudwatch.EC2.UCount"] = payload.data.actor.account.ec2uc.results[0].uc
        cres.ucresult["Incident.Open.UCount"] = payload.data.actor.account.aiiopenuc.results[0].uc
        cres.ucresult["Incident.Close.UCount"] = payload.data.actor.account.aiicloseuc.results[0].uc
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
function LoopAccounts(sCred, aAccounts, nBatchID){
  parray = []
  for(oa of aAccounts){
    parray.push(GetCounts(sCred,oa,nBatchID))
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

//stdout logger
function logit(mname,msg, ...theargs){
  if(mname == null || msg == null){throw('logit(method,msg,...) requires at least 2 params')}
  console.log(`[${(new Date()).toISOString()}]${mname}()-> ${msg}${theargs.length == 0 ? '' : ':'}`,...theargs)
}

function ProcessMasterContext(sMasterCred, nBatchID, sTargetTable){
  var nstrt = Date.now()
  return GetAccounts(sMasterCred)
  .then((aAccounts) =>{
    //console.log('GetAccounts() result:',payload)
    logit('ProcessMasterContext','working on number of accounts',aAccounts.length)
    //logit('main','aAccounts',aAccounts)
    return LoopAccounts(sMasterCred, aAccounts, nBatchID)
  }).then((aCounts)=>{
    var uCounts = []
    aCounts.forEach(el => {uCounts.push(el.ucresult)})
    return PostEvents($secure.POA_INGEST_KEY,$secure.POA_ACCOUNT_ID,sTargetTable,nBatchID,uCounts)
    .then((response1)=>{
      logit('main','postevents-response1',response1.statusCode)
    })
  }).then(()=>{
    logit('ProcessMasterContext','iteration stopwatch in secs',(Date.now()-nstrt)/1000)
  }).catch(err => {
    logit('ProcessMasterContext','caught error on promise-chain',err)
    assert.fail(err)
  })
}

//force serial processing of Master-Context
logit('main','starting the run')
var sTargetTable = GRollupTable
$util.insights.set('BatchId',GBatchID.toString())
$util.insights.set('EntityTargetTable',sTargetTable)

//use reduce() to serialize walking Master Contexts
GCredArray.reduce(function(pr, sMasterCred) {
  return pr.then(function() {
    return ProcessMasterContext(sMasterCred,GBatchID,sTargetTable)
  })
}, Promise.resolve())
