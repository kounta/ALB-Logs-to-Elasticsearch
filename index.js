/*
 * This project based on https://github.com/awslabs/amazon-elasticsearch-lambda-samples
 * & https://github.com/blmr/aws-elb-logs-to-elasticsearch.git
 *
 * Function for AWS Lambda to get AWS ELB log files from S3, parse
 * and add them to an Amazon Elasticsearch Service domain.
 *
 * Copyright 2015- Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at http://aws.amazon.com/asl/
 * or in the "license" file accompanying this file.  This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

/* Imports */
const AWS = require('aws-sdk')
const LineStream = require('byline').LineStream
const stream = require('stream')
const zlib = require('zlib')
const ES = require('elasticsearch')
const url = require('url')

/* Globals */
var indexTimestamp
var esDomain
var elasticsearch
const s3 = new AWS.S3()
// Bulk indexing and stats
var totalIndexedLines = 0
var totalStreamedLines = 0
var bulkBuffer = []
var bulkTransactions = 0
// ES configs
const esTimeout = 100000
const esMaxSockets = 20

/* Lambda "main": Execution starts here */
exports.handler = (event, context) => {
  console.log("Hello")
  // Set indexTimestamp and esDomain index fresh on each run
  indexTimestamp = new Date().toISOString().replace(/-/g, '.').replace(/T.+/, '')

  esDomain = {
    endpoint: process.env.ES_ENDPOINT,
    index: process.env.ES_INDEX_PREFIX + '-' + indexTimestamp, // adds a timestamp to index. Example: alblogs-2015.03.31
    doctype: process.env.ES_DOCTYPE,
    maxBulkIndexLines: process.env.ES_BULKSIZE // Max Number of log lines to send per bulk interaction with ES
  }

  /**
     * Get connected to Elasticsearch using the official elasticsearch.js
     * client.
     */
  elasticsearch = new ES.Client({
    host: esDomain.endpoint,
    apiVersion: '6.3',
    // connectionClass: require('http-aws-es'),
    log: 'error',
    requestTimeout: esTimeout,
    maxSockets: esMaxSockets
  })

  console.log("Connected to ES")

  // Prepare bulk buffer
  initBulkBuffer()

  /* == Streams ==
     * To avoid loading an entire (typically large) log file into memory,
     * this is implemented as a pipeline of filters, streaming log data
     * from S3 to ES.
     * Flow: S3 file stream -> Log Line stream -> Log Record stream -> Lambda buffer -> ES Bulk API
     */
  const lineStream = new LineStream()

  // A stream of log records, from parsing each log line
  const recordStream = new stream.Transform({
    objectMode: true
  })

  // We want `this` shared for the function
  recordStream._transform = function transform(line, encoding, done) {
    // console.log("Transforming line")
    const logRecord = parse(line.toString())

    const serializedRecord = JSON.stringify(logRecord)
    this.push(serializedRecord)

    totalStreamedLines++
    done()
  }
  event.Records.forEach((record) => {
    const bucket = record.s3.bucket.name
    const objKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '))
    s3LogsToES(bucket, objKey, context, lineStream, recordStream)
  })
}

/*
 * Get the log file from the given S3 bucket and key.  Parse it and add
 * each log record to the ES domain.
 *
 * Note: The Lambda function should be configured to filter for
 * .log.gz files (as part of the Event Source "suffix" setting).
 */
const s3LogsToES = (bucket, key, context, lineStream, recordStream) => {
  console.log(`Processing s3://${bucket}/${key}`)
  const s3Stream = s3.getObject({
    Bucket: bucket,
    Key: key
  }).createReadStream()

  const gunzipStream = zlib.createGunzip()

  s3Stream
    .pipe(gunzipStream)
    .pipe(lineStream)
    .pipe(recordStream)
    .on('data', (parsedEntry) => {
      // Add this log entry to the buffer
      addToBulkBuffer(parsedEntry)

      // See if it's time to flush and proceed
      checkFlushBuffer()
    })
    .on('error', () => {
      console.log(
        'Error getting object "' + key + '" from bucket "' + bucket + '".  ' +
                'Make sure they exist and your bucket is in the same region as this function.')
      context.fail()
    })
    .on('finish', () => {
      flushBuffer()
      console.log('Process complete. ' + totalIndexedLines + ' out of ' + totalStreamedLines + ' added in ' + bulkTransactions + ' transactions.')
      context.succeed()
    })
}

/*
 * Bulk Buffering Functions
 */
const initBulkBuffer = () => {
  bulkBuffer = []
}

const addToBulkBuffer = (doc) => {
  bulkBuffer.push(doc)
}

const checkFlushBuffer = () => {
  if (bulkBuffer.length >= esDomain.maxBulkIndexLines) {
    flushBuffer()
  }
}

const flushBuffer = () => {
  // console.log("Sending buffer to ES")
  // Map the raw lines into an ES bulk transaction body
  const bulkBody = convertBufferToBulkBody(bulkBuffer)

  // Submit to ES
  postBulkDocumentsToES(bulkBody)

  // Keep stats
  const numLines = bulkBody.length / 2
  totalIndexedLines += numLines
  bulkTransactions++

  // Clear the buffer
  initBulkBuffer()
}

const convertBufferToBulkBody = (buffer) => {
  let bulkBody = []

  for (const i in buffer) {
    const logEntry = buffer[i]

    bulkBody.push({ index: { _index: esDomain.index, _type: esDomain.doctype } })
    bulkBody.push(logEntry)
  }

  return bulkBody
}

const postBulkDocumentsToES = (bulkBody) => {
  console.log(`Posting ${JSON.stringify(bulkBody)}`)
  elasticsearch.bulk({ body: bulkBody })
}

/**
 * Line Parser.
 * It would have been much easier with Logstash Grok...
 */
const parse = (line) => {
  // Fields in log lines are essentially space separated,
  // but are also quote-enclosed for strings containing spaces.
  const fieldNames = [
    'type',
    '@timestamp',
    'elb',
    'client',
    'target',
    'request_processing_time',
    'target_processing_time',
    'response_processing_time',
    'elb_status_code',
    'target_status_code',
    'received_bytes',
    'sent_bytes',
    'request',
    'user_agent',
    'ssl_cipher',
    'ssl_protocol',
    'target_group_arn',
    'trace_id',
    'domain_name',
    'chosen_cert_arn',
    'matched_rule_priority',
    'request_creation_time',
    'actions_executed',
    'redirect_url',
    'error_reason'
  ]

  // First phase, separate out the fields
  let withinQuotes = false
  let currentField = 0
  let currentValue = ''
  let currentNumeric = NaN

  let parsed = {}

  // Remove trailing newline
  if (line.match(/\n$/)) {
    line = line.slice(0, line.length - 1)
  }

  // Character by character
  for (const i in line) {
    const c = line[i]

    if (!withinQuotes) {
      if (c === '"') {
        // Beginning a quoted field.
        withinQuotes = true
      } else if (c === ' ') {
        // Separator. Moving on to the next field.

        // Convert to numeric type if appropriate.
        // This is needed to make sure Elasticsearch gets the
        // dynamic mapping correct.
        currentNumeric = Number(currentValue)
        if (!isNaN(currentNumeric)) {
          currentValue = currentNumeric
        }

        // Save current and reset.
        const fieldName = fieldNames[currentField]
        parsed[fieldName] = currentValue
        currentField++
        currentValue = ''
      } else {
        // Part of this field.
        currentValue += c
      }
    } else {
      if (c === '"') {
        // Ending a quoted field.
        withinQuotes = false
      } else {
        // Part of this quoted field.
        currentValue += c
      }
    }
  }

  // Save off the last one
  parsed[fieldNames[currentField]] = currentValue

  // Second phase, cleanups.

  // Breaking out the port for the client and target, if there's a colon present
  const colonSep = ['client', 'target']
  for (let i in colonSep) {
    const orig = parsed[colonSep[i]]

    if (orig.indexOf(':') > 0) {
      const splat = orig.split(':')
      parsed[colonSep[i]] = splat[0]
      parsed[colonSep[i] + '_port'] = Number(splat[1])
    }
  }

  // Don't put '-' into fields that are otherwise numbers
  if (parsed['matched_rule_priority'] === '-') {
    delete parsed['matched_rule_priority']
  }
  if (parsed['target_status_code'] === '-') {
    delete parsed['target_status_code']
  }
  if (parsed['undefined']) delete parsed['undefined']

  // Third phase, parsing out the request into more fields
  // Only do this if there's actually data in that field
  if (parsed['request'].trim() !== '- - -') {
    const splat = parsed['request'].split(' ')

    // Basic values
    parsed['request_method'] = splat[0]
    parsed['request_uri'] = splat[1]
    parsed['request_http_version'] = splat[2]

    // If we can parse the URL, we can populate other fields properly
    try {
      const uri = url.parse(splat[1])
      // Strip trailing colon here, it seems to be left in sometimes?
      parsed['request_uri_scheme'] = uri.protocol ? uri.protocol.replace(/:$/, '') : ''
      parsed['request_uri_host'] = uri.hostname ? uri.hostname : ''
      parsed['request_uri_port'] = uri.port ? parseInt(uri.port) : 0
      parsed['request_uri_path'] = uri.pathname ? uri.pathname : ''
      parsed['request_uri_query'] = uri.query ? uri.query : ''
    } catch (e) {}
  }

  // All done.
  return parsed
}
