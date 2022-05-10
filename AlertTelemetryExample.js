// Library Defs
var assert = require('assert')
var got = require('got')
// Global Defs
const GRAPH_API = 'https://api.newrelic.com/graphql'
const TARGET_ACCOUNT = '1977586'
const TELEMETRY_QUERY = 'FROM Metric SELECT average(host.cpuPercent) as avg  SINCE 20 minutes ago'
const AVG_THRESHOLD = 1
const NERDGRAPH_QUERY = `
{
  actor {
    account(id: TARGETACCOUNT) {
      nrql(query: "TARGETQUERY") {
        results
      }
    }
  }
}`
// Function Defs
async function getTelemetry(sTargetAccountId, sTargetNRQLQuery, sTargetNerdGraphQuery) {
  var q = sTargetNerdGraphQuery.replace('TARGETACCOUNT',sTargetAccountId).replace('TARGETQUERY',sTargetNRQLQuery)
  console.log('nerdgraph',q)
  var opts = {
    url: GRAPH_API,
    headers: {'Content-Type': 'application/json', 'x-API-KEY':  $secure.USER_API_KEY},
    json: {'query': q, 'variables': {}}
  }

  let resp = await got.post(opts).json()
  //debug logging
  //console.log('response',resp)

  if(resp.errors && resp.errors.count > 0){
    assert.failed(`getTelemetry()->nerdgraph failed with response [[[${resp.errors[0].message}]]]`)
  }

  return resp.data.actor.account.nrql.results
}

async function main(sTargetAccountId, sTargetNRQLQuery, sTargetNerdGraphQuery, nThresh) {
  var opayload = await getTelemetry(sTargetAccountId, sTargetNRQLQuery, sTargetNerdGraphQuery)
  //debug logging
  console.log('nrql results',opayload[0].avg,nThresh,opayload)
  assert(opayload[0].avg < nThresh, `average cpu of [${opayload[0].avg}] exceeded threshold of [${nThresh}]`)
}

main(TARGET_ACCOUNT, TELEMETRY_QUERY, NERDGRAPH_QUERY, AVG_THRESHOLD)
