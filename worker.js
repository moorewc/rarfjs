const { workerData, parentPort } = require('worker_threads')
const IsilonClient = require('@moorewc/node-isilon')
const async = require('async')
const chalk = require('chalk')
require('log-timestamp')

const { config, name, id, concurrency, log_level } = workerData
const isilon = new IsilonClient(config)

let numFiles; let numUpdates = 0
let job = {}

function logger(string) {
  console.log('[' + chalk.blue(name) + `] ${string}`)
}

function completeJob({ shutdown = false }) {
  job.completedAt = new Date();

  parentPort.postMessage({ msg: 'results', id: id, results: job, shutdown: shutdown })

  if (!shutdown) {
    parentPort.postMessage({ msg: 'next', id: id, name: name })
  }
}

async function walkTree({ path, user }) {

  fileQueue.push({ path: path, user: user })

  const response = isilon.namespace.get(path.path)

  for (const result of await response.readdir()) {


    if (result.type === 'container') {
      await walkTree({ path: result, user: user })
    } else {
      fileQueue.push({ path: result, user: user })
    }
  }
}

const fileQueue = async.queue(async ({ path, user }, callback) => {
  let response

  if (log_level >= 3) {
    let type = path.type === "container" ? "D" : "F"
    logger(`SCANNING ${type} ${path.path}`)
  }


  filesScanned++

  try {
    response = await path.getAcl()
  } catch (error) {
    throw error
  }

  if (path.type === 'container') {
    job.stats.dirsScanned++;
  } else {
    job.stats.filesScanned++;
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
    if (path.type === 'container') {
      job.stats.dirsFixed++;
    } else {
      job.stats.filesFixed++;
    }

    numUpdates++

    await path.setAcl(response);

    if (log_level >= 2) {
      logger(chalk.yellow.bold('REPAIRED') + ` ${path.path} (${user.id.id})`)
    }
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



parentPort.on('message', async ({ cmd, path, user }) => {
  if (cmd === 'interrupt') {
    completeJob({ shutdown: true });
  }

  if (cmd === 'shutdown') {
    logger("SHUTTING DOWN");
    process.exit();
  }

  if (cmd === 'process_user') {
    if (user) {
      logger(`PROCESSING ${path} (${user.id.id})`)

      // The path variable is sent as a string from master process and the
      // rest of the workflow expects an isilon namespace object.
      const _path = await isilon.namespace.get(path)

      job = {
        path: path,
        user: user,
        startedAt: new Date(),
        completedAt: undefined,
        stats: {
          filesScanned: 0,
          dirsScanned: 0,
          filesFixed: 0,
          dirsFixed: 0
        }
      }

      // Record the start and time deltas
      startedAt = new Date()
      filesScanned = numUpdates = 0
      dates = 0



      await walkTree({ path: _path, user: user })


      await fileQueue.drain()

      deltaTime = ((new Date() - startedAt) / 1000).toFixed(2)
      iops = Math.round((filesScanned + numUpdates) / deltaTime).toFixed(0)
      filesP = (numUpdates / (filesScanned + numUpdates) * 100).toFixed(2)

      pathColor = chalk.hex('#FF0000')

      //    logger(chalk.green('COMPLETED') + ` ${path} [Scanned: ${filesScanned}, Updated:  ${numUpdates} (${filesP}%), Time: ${deltaTime}s, IOPs: ${iops}]`)
    } else {
      logger(chalk.yellow('SKIPPING') + ` ${path} (USER NOT FOUND)`)
    }
    //    parentPort.postMessage({ msg: 'next', id: id, name: name })
    // Request a new path from the master process.

    completeJob({ shutdown: false });
  }
})