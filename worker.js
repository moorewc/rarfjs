const { workerData, parentPort } = require('worker_threads')
const IsilonClient = require('@moorewc/node-isilon')
const async = require('async')
const chalk = require('chalk')
require('log-timestamp')

const { config, name, id, concurrency, log_level } = workerData
const isilon = new IsilonClient(config)

let numFiles; let numUpdates = 0

function logger(string) {
  console.log('[' + chalk.blue(name) + `] ${string}`)
}

async function walkTree({ path, user }) {
  const results = []

  fileQueue.push({ path: path, user: user })

  const response = isilon.namespace.get(path.path)

  for (const result of await response.readdir()) {
    fileQueue.push({ path: result, user: user })

    if (result.type === 'container') {
      await walkTree({ path: result, user: user })
    }
  }
}

const fileQueue = async.queue(async ({ path, user }, callback) => {
  let response

  if (log_level >= 3) {
    logger(`SCANNING ${path.path}`)
  }

  filesScanned++
  try {
    response = await path.getAcl()
  } catch (error) {
    throw error
  }

  changed = false
  for (acl of response.acl) {
    if (acl.trustee.name === 'root') {
      changed = true
      acl.trustee.name = user.id.name
      acl.trustee.id = user.id.id
    }
  }

  if (response.owner.name === 'root') {
    changed = true
    response.owner.name = user.id.name
    response.owner.id = user.id.id
  }

  if (changed == true) {
    if (log_level >= 2) {
      logger(chalk.yellow.bold('REPAIRED') + ` ${path.path} (${user.id.id})`)
    }

    numUpdates++
    await path.setAcl(response)
  }
}, concurrency)

async function getFolderAcl(path) {
  let results

  try {
    results = await path.getAcl()
  } catch (error) {
    throw error
  }
}

logger(`Connecting to ${config.ssip}`)

parentPort.postMessage({ msg: 'next', id: id, name: name })

parentPort.on('message', async ({ path, user }) => {
  if (user) {
    logger(`PROCESSING ${path} (${user.id.id})`)

    // The path variable is sent as a string from master process and the
    // rest of the workflow expects an isilon namespace object.
    const _path = await isilon.namespace.get(path)

    // Record the start and time deltas
    startedAt = new Date()
    filesScanned = numUpdates = 0
    dates = 0
    await walkTree({ path: _path, user: user })

    if (fileQueue.length() > 0) {
      await fileQueue.drain()
    }

    deltaTime = ((new Date() - startedAt) / 1000).toFixed(2)
    iops = Math.round((filesScanned + numUpdates) / deltaTime).toFixed(0)
    filesP = (numUpdates / (filesScanned + numUpdates) * 100).toFixed(2)

    pathColor = chalk.hex('#FF0000')

    logger(chalk.green('COMPLETED') + ` ${path} [Scanned: ${filesScanned}, Updated:  ${numUpdates} (${filesP}%), Time: ${deltaTime}s, IOPs: ${iops}]`)
  } else {
    logger(chalk.yellow('SKIPPING') + ` ${path} (USER NOT FOUND)`)
  }

  // Request a new path from the master process.
  parentPort.postMessage({ msg: 'next', id: id, name: name })
})
