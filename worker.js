const { workerData, parentPort } = require('worker_threads')
const IsilonClient = require('@moorewc/node-isilon')
const async = require('async')
const chalk = require('chalk')
require('log-timestamp')

const { config, name, id, concurrency, log_level } = workerData
const isilon = new IsilonClient(config)
const axios = isilon.ssip.axios

let job = {}

function logger(string) {
  console.log('[' + chalk.blue(name) + `] ${string}`)
}

function completeJob({ shutdown = false }) {
  job.completedAt = new Date();

  parentPort.postMessage({ msg: 'update_results', id: id, results: job, shutdown: shutdown })

  if (!shutdown) {
    parentPort.postMessage({ msg: 'close_open_files', id: id, name: name })
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

  let SIDS = [
    'S-1-5-21-2042961196-1878016268-396375212-407955',
    'S-1-5-21-2042961196-1878016268-396375212-407877',
    'S-1-5-21-2042961196-1878016268-396375212-475325',
    'S-1-5-21-2042961196-1878016268-396375212-407802',
    'S-1-5-21-2042961196-1878016268-396375212-407708',
    'S-1-5-21-2042961196-1878016268-396375212-570187',
    'S-1-5-21-2042961196-1878016268-396375212-407705'
  ];

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
    if (acl.trustee.name === 'root' || acl.trustee.id in SIDs) {
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

async function getGroup({ group: group }) {
  let baseUrl = `/platform/11/auth/groups/${group}`

  try {
    let response = await axios.get(baseUrl);

    return response.data.groups[0];
  } catch (error) {
    if (error.status === 404) {
      return undefined
    }

    throw error
  }
}

async function removeUserFromGroup({ user, group }) {
  let baseUrl = `/platform/11/auth/groups/${group}/members/${user.id.id}`

  let g = await getGroup({ group: group });

  if (g === undefined) {
    return undefined
  }

  try {
    let response = await axios.delete(baseUrl);
    return true;
  } catch (error) {

    return false;
  }
}

async function addUserToGroup({ user, group }) {
  let baseUrl = `/platform/11/auth/groups/${group}/members`

  let g = await getGroup({ group: group });

  if (g === undefined) {
    return undefined
  }

  try {
    let response = await axios.post(baseUrl, {
      type: "user",
      id: user.id.id
    });
    return true;
  } catch (error) {
    return false;
  }
}

logger(`CONNECTING TO ${config.ssip}`)

parentPort.on('message', async ({ cmd, path, user, uname }) => {
  if (cmd === 'start_processing') {
    parentPort.postMessage({ msg: 'close_open_files', id: id, name: name })
  }
  if (cmd === 'interrupt') {
    completeJob({ shutdown: true });
  }

  if (cmd === 'shutdown') {
    logger("SHUTTING DOWN");
    process.exit();
  }

  if (cmd === 'repair_permissions') {
    if (user) {
      logger(`PROCESSING ${path} (${user.id.name} :: ${user.id.id})`)

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

      try {
        let groupName = 'Run-As-Root';
        let result = await removeUserFromGroup({ user: user, group: groupName })
        if (result === undefined) {
          logger(`Unable to remove ${user.id.name} from group '${groupName}', group does not exist.`)
        } else if (result === true) {
          logger(`REMOVED USER ${user.id.name} [${user.id.id}] FROM GROUP 'LOCAL:${groupName}'`)
        }
      } catch (error) {
        throw error;
      }

      // Add user to group to disable access.
      try {
        let groupName = 'Blocked-Users';
        let result = await addUserToGroup({ user: user, group: groupName });

        if (result === undefined) {
          logger(`Unable to add ${user.id.name} from group '${groupName}', group does not exist.`)
        } else if (result === true) {
          logger(`ADDED USER ${user.id.name} [${user.id.id}] TO GROUP 'LOCAL:${groupName}'`)
        }
      } catch (error) {
        throw error
      }

      try {
        await walkTree({ path: _path, user: user })
      } catch (error) {
        throw error;
      }


      try {
        await fileQueue.drain()
      } catch (error) {
        throw error;
      }

      // Remove User from Blocked-Users group to re-enable access.
      try {
        let groupName = 'Blocked-Users';
        let result = await removeUserFromGroup({ user: user, group: groupName });

        if (result === undefined) {
          logger(`Unable to remove ${user.id.name} from group '${groupName}', group does not exist.`)
        } else if (result === true) {
          logger(`REMOVED USER ${user.id.name} [${user.id.id}] FROM GROUP 'LOCAL:${groupName}'`)
        }
      } catch (error) {
        throw error
      }
    } else {
      logger(chalk.yellow('SKIPPING') + ` ${path} (USER NOT FOUND)`)
    }
    completeJob({ shutdown: false });
  }
});