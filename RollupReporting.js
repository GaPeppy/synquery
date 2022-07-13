 var got = require('got')

const GBatchID = Date.now()
const assert = require('assert')
const zlib = require('zlib')
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
async function NerdGraph(sngcred,squery,svar){
    const options = {url: 'https://api.newrelic.com/graphql', json:{query:squery,variables:svar}, headers:{'Api-key':sngcred}}
    //logit('NerdGraph','options',options)
    try {
    const response = await got.post(options).json()
    //debug
    //logit('NerdGraph','response',response)
    if(response.errors !== undefined){
      logit('NerdGraph','response.errors',squery, svar, sngcred.substring(0,10),response.errors)
      throw new Error('NerdGraph failed')
    }
    return response
  } catch (e) {
    logit('NerdGraph','caught exception',JSON.stringify(e))
    throw e
  }
}
function CaptureCustomAttribute(sMCred,sTag,sValue){
  $util.insights.set('MasterCred.' + sTag + '.' + sMCred.substring(0,10), sValue)
}
//
//
//
async function zippayload(sTable, ntimestamp, sprovider, aEvents){
  return await new Promise((resolve, reject) => {
    if(!Array.isArray(aEvents) || aEvents.length == 0){return reject('array is empty')}
    logit('zippayload','about to create number of events',aEvents.length,sTable)
    for (var oa of aEvents){
      oa.eventType = sTable
      oa.timestamp = ntimestamp
      oa.provider = sprovider
    }
    zlib.gzip(JSON.stringify(aEvents), null, function (err, compressed_json) {
      if(err) {logit('zippayload','gzip bombed',err); return reject(err)}
      logit('zippayload','compressed size',compressed_json.byteLength)
      return resolve(compressed_json)
    })
  })
}
//
// PostZipEvents() -> Push results into custom Event Table
//
async function PostZipEvents(sCred,sTargetAcctId,zippedpl){
  const options = {url: `https://insights-collector.newrelic.com/v1/accounts/${sTargetAcctId}/events`,
                  body:zippedpl,
                  headers:{'Content-Encoding': 'gzip','Api-key':sCred}}
    //debug line
    //logit('PostEvents','options',options)
  try {
    return await got.post(options).json()
  } catch (e) {
    logit('PostZipEvents','caught exception',JSON.stringify(e))
    throw e
  }
}
//
// GetAccounts() -> Get list of subaccounts for the current Master Context
//{ data: { actor: { accounts: [Array] } } }
async function GetAccounts(sCred){
    const CQuery = `{
      actor {
        accounts(scope: IN_REGION) {
          id
          name
        }
      }
    }`
    //logit('GetAccounts','creds',sCred.substring(0,10)+'****')
    const payload = await NerdGraph(sCred,CQuery,'')
    var aAccounts = payload.data.actor.accounts
    CaptureCustomAttribute(sCred,'Accounts.Count',aAccounts.length)
    return aAccounts
}
//
// GetCounts() -> run parallel queries for a target subaccount
//
async function GetCounts1(sCred,oAcct,nBatchID, oResult){
  const METHODNAME = 'GetCounts1'
  const CQuery = `
{
  actor {
    dbcount: entitySearch(query: "type = 'DASHBOARD' and accountId = 'RPMID'") {
      count
    }
    iacount: entitySearch(query: "type = 'HOST' and accountId = 'RPMID'  and reporting = 'true'") {
      count
    }
    account(id: RPMID) {
      lambdauc: nrql(timeout: 40, query: "SELECT uniqueCount(entityGuid) as uc FROM AwsLambdaInvocation WHERE provider = 'LambdaFunction' SINCE 1 day ago") {
        results
      }
      lambdaciuc: nrql(timeout: 40, query: "SELECT uniqueCount(entityGuid) as uc from ServerlessSample where provider = 'LambdaFunction' SINCE 1 day ago") {
        results
      }
      ec2uc: nrql(timeout: 40, query: "SELECT uniqueCount(entityGuid) as uc FROM ComputeSample WHERE provider = 'Ec2Instance' SINCE 1 day ago") {
        results
      }
      k8snodeuc: nrql(timeout: 40, query: "FROM K8sNodeSample SELECT uniqueCount(entityGuid) as uc SINCE 1 day ago") {
        results
      }
      iauc: nrql(timeout: 40, query: "SELECT uniqueCount(entityGuid) as uc FROM SystemSample SINCE 1 day ago") {
        results
      }
      aiiopenuc: nrql(timeout: 40, query: "SELECT uniqueCount(incidentId) as uc from NrAiIncident where event = 'open' since 1 day ago") {
        results
      }
      aiicloseuc: nrql(timeout: 40, query: "SELECT uniqueCount(incidentId) as uc from NrAiIncident where event = 'close' since 1 day ago") {
        results
      }
      peakdailydpm: nrql(timeout: 40, query: "select max(AccountDPM) as peakdailydpm from (FROM Metric select rate(sum(newrelic.resourceConsumption.currentValue), 1 minute) as AccountDPM  where limitName ='Metric API data points per minute (DPM)' and limitTimeInterval =  '1 minute' timeseries 30 minutes ) since 1 day ago") {
        results
      }
      showeventtypes: nrql(timeout: 40, query: "show eventtypes since 1 day ago") {
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

  const payload = await NerdGraph(sCred,CQuery.replace(/RPMID/g,oAcct.id),'')
  try{
    oResult["NrAccountId"]     = oAcct.id
    oResult["NrAccountName"]   = oAcct.name
    oResult["ReportingPeriod"] = '1d'
    oResult["BatchId"]         = nBatchID.toString()
    oResult["Metadata.Dashboard.Count"]  = payload.data.actor.dbcount.count
    oResult["Metadata.InfraAgent.Count"] = payload.data.actor.iacount.count
    oResult["InfraAgent.UCount"]         = payload.data.actor.account.iauc.results[0].uc
    oResult["InfraAgent.K8sNode.UCount"] = payload.data.actor.account.k8snodeuc.results[0].uc
    oResult["Serverless.Lambda.UCount"]  = payload.data.actor.account.lambdauc.results[0].uc
    oResult["Cloudwatch.Lambda.UCount"]  = payload.data.actor.account.lambdaciuc.results[0].uc
    oResult["Cloudwatch.EC2.UCount"]     = payload.data.actor.account.ec2uc.results[0].uc
    oResult["Incident.Open.UCount"]      = payload.data.actor.account.aiiopenuc.results[0].uc
    oResult["Incident.Close.UCount"]     = payload.data.actor.account.aiicloseuc.results[0].uc
    oResult["Metric.PeakDPM"]            = payload.data.actor.account.peakdailydpm.results[0].peakdailydpm
    oResult["Show.EventTypes.Count"]     = payload.data.actor.account.showeventtypes.results.length
    oResult["CloudIntegrations.LinkedAccounts.Count"] = payload.data.actor.account.cloud.linkedAccounts.length
    //loop over limits and capture the count of limit events in last 24hours
    /**
      limiterrors: nrql(query: "select count(*) from NrIntegrationError where category = 'RateLimit' facet limitName limit 100 since 1 day ago") {
        results
      }
        payload.data.actor.account.limiterrors.results.forEach( el => {
          cres.ucresult["RateLimit." + el.limitName] = el.count
        })
    **/
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
        oResult["CloudIntegrations.Services.Count"] = nServices

        //logit(METHODNAME,'debug cres',cres)
        return oResult
  }
  catch (e) {
    logit(METHODNAME,'caught exception',oAcct,e,payload)
    return reject(e)
  }
}
//
// GetCounts() -> run parallel queries for a target subaccount
//
async function GetCounts2(sCred,oAcct,nBatchID, oResult){
  const METHODNAME = 'GetCounts2'
  const CQuery = `
{
  actor {
    account(id: RPMID) {
      limiterrors: nrql(query: "select count(*) from NrIntegrationError where category = 'RateLimit' facet limitName limit 100 since 1 day ago") {
        results
      }
      logcount: nrql(query: "select count(*) from Log since 1 day ago") {
        results
      }
    }
  }
}`

  const payload = await NerdGraph(sCred,CQuery.replace(/RPMID/g,oAcct.id),'')
  try{
        oResult["NrAccountId"]     = oAcct.id
        oResult["NrAccountName"]   = oAcct.name
        oResult["ReportingPeriod"] = '1d'
        oResult["BatchId"]         = nBatchID.toString()
        oResult["Log.Count"]       = payload.data.actor.account.logcount.results[0].count
        //loop over limits and capture the count of limit events in last 24hours
        payload.data.actor.account.limiterrors.results.forEach( el => {
          oResult["RateLimit." + el.limitName] = el.count
        })

        //logit(METHODNAME,'debug cres',cres)
        return oResult
  }
  catch (e) {
    logit(METHODNAME,'caught exception',oAcct,e,payload)
    return e
  }
}
//
// LoopAccounts() -> scatter and gather for current MasterContext; parallel execution
//
async function LoopAccounts(sCred, aAccounts, nBatchID, aResults){
  parray = []
  for(oa of aAccounts){
    var oResult = {}
    aResults.push(oResult)
    parray.push(GetCounts1(sCred,oa,nBatchID, oResult))
    parray.push(GetCounts2(sCred,oa,nBatchID, oResult))
  }
  return Promise.all(parray)
}
//
// currently not used
//
async function LoopAccountsSerial(sCred, aAccounts){
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

async function ProcessMasterContext(sMasterCred, nBatchID, sTargetTable){
  const METHODNAME = 'ProcessMasterContext'
  var nstrt = Date.now()
  var aAccountResponseObjects = []
  const aAccounts = await GetAccounts(sMasterCred)

  //console.log('GetAccounts() result:',payload)
  logit('ProcessMasterContext','working on number of accounts',aAccounts.length)
  //logit(METHODNAME,'aAccounts',aAccounts)
  // called methods will populate the array -> aAccountResponseObjects
  await LoopAccounts(sMasterCred, aAccounts, nBatchID, aAccountResponseObjects)
  logit(METHODNAME,'final acct array',aAccountResponseObjects)

  const zp = await zippayload(sTargetTable, nBatchID, 'eroll', aAccountResponseObjects)
  logit(METHODNAME,'post zip type', typeof(zp))
  const response1 = await PostZipEvents($secure.POA_INGEST_KEY,$secure.POA_ACCOUNT_ID,zp)
  logit(METHODNAME,'postevents-response1',response1.statusCode)

  logit(METHODNAME,'iteration stopwatch in secs',(Date.now()-nstrt)/1000)
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
