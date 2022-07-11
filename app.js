const { Worker } = require('worker_threads')
const { ArgumentParser } = require('argparse')
const { exit } = require('process')
const fs = require('fs')
const lineReader = require('line-reader');
const prompts = require('prompts')
const IsilonClient = require('@moorewc/node-isilon')
const async = require('async')
const chalk = require('chalk')
var vsprintf = require('sprintf-js').vsprintf;
var heredoc = require('heredoc')

let report = {
  jobs: [],
  startedAt: new Date(),
  completedAt: undefined,
  stats: {
    filesScanned: 0,
    filesFixed: 0,
    dirsScanned: 0,
    dirsFixed: 0
  }
}

PAPI_SESSIONS = [];

let repairPath = '';

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

var reportHeader = heredoc.strip(function () {/*
+==========================+=============+===============+==============================================================+========================+========================+=========+
| Completed At             | Status      | User          | Path                                                         | Files                  | Folders                | Time    |
+--------------------------+-------------+---------------+--------------------------------------------------------------+------------------------+------------------------+---------+*/
});

function printFinalReport() {
  let interrupted = false;

  report.jobs.sort(
    (a, b) => {
      deltaA = a.completedAt - a.startedAt;
      deltaB = b.completedAt - b.startedAt;

      if (deltaA < deltaB) return 1;
      if (deltaA > deltaB) return -1;

      return 0;
    }
  )

  console.log(reportHeader);

  report.jobs.map((j) => {
    if (j.status) {


      status = j.status.replace(/[\u001b\u009b][[()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nqry=><]/g, '');

      if (status === 'INTERRUPTED') {
        interrupted = true;
      }

      return {
        Status: status,
        User: j.user.id.name,
        SID: j.user.id.id,
        Path: j.path,
        Files: vsprintf('%7d/%7d (%3d%%)', [j.stats.filesFixed, j.stats.filesScanned, ((j.stats.filesFixed / j.stats.filesScanned) * 100).toFixed(2)]),
        Folders: vsprintf(`%7d/%7d (%3d%%)`, [j.stats.dirsFixed, j.stats.dirsScanned, ((j.stats.dirsFixed / j.stats.dirsScanned) * 100).toFixed(2)]),
        Time: ((j.completedAt - j.startedAt) / 1000).toFixed(2),
        CompletedAt: j.completedAt.toISOString()
      }
    }
  }).forEach(j => {
    if (j) {
      console.log(vsprintf('| %s | %-11s | %-13s | %-60s | %22s | %22s | %6.2fs |', [j.CompletedAt, j.Status, j.User, j.Path, j.Files, j.Folders, j.Time]));
    }
  });
  console.log("+--------------------------+-------------+---------------+--------------------------------------------------------------+------------------------+------------------------+---------+")

  report.completedAt = new Date();
  deltaTime = ((report.completedAt - report.startedAt) / 1000).toFixed(2);

  iops = 0;
  for (key of Object.keys(report.stats)) {
    iops += report.stats[key];
  }
  iops = (iops / deltaTime).toFixed(0);

  let fileStats = vsprintf('%7d/%7d (%3d%%)', [report.stats.filesFixed, report.stats.filesScanned, ((report.stats.filesFixed / report.stats.filesScanned) * 100)]);
  let dirStats = vsprintf('%7d/%7d (%3d%%)', [report.stats.dirsFixed, report.stats.dirsScanned, ((report.stats.dirsFixed / report.stats.dirsScanned) * 100)]);

  console.log(vsprintf('| %-24s | %-11s | %-13s | %-60s | %22s | %22s | %6.2fs |', [report.completedAt.toISOString(), interrupted ? 'INTERRUPTED' : 'COMPLETED', 'ALL USERS', repairPath, fileStats, dirStats, deltaTime]));
  console.log("+--------------------------+-------------+---------------+--------------------------------------------------------------+------------------------+------------------------+---------+")

  let avgTime = (deltaTime / report.jobs.length).toFixed(2);

  console.log(`\nNum Users:  ${report.jobs.length}`);
  console.log(`Average Time:  ${avgTime}s`)
}

function logger(string) {
  let timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [` + chalk.yellow('MASTER001') + `] ${string} `)
}

async function GetCredentials() {
  questions = [
    {
      type: 'text',
      name: 'username',
      message: 'Username:'
    },
    {
      type: 'password',
      name: 'password',
      message: 'Password:'
    }
  ]

  if (process.env.ISI_USERNAME && process.env.ISI_PASSWORD) {
    return {
      username: process.env.ISI_USERNAME,
      password: process.env.ISI_PASSWORD
    }
  }

  return await prompts(questions)
}

function GetArguments() {
  const parser = new ArgumentParser({
    description: 'Argparse example',
    add_help: true
  })

  parser.add_argument('--hostname', { required: true, help: 'IP Address or Hostname for System Access Zone.' })
  parser.add_argument('--path', {
    required: true,
    help: 'The /ifs path where FolderRedirect share is located.'
  })
  parser.add_argument('--threads', {
    required: false,
    help: 'Number of Threads to run.',
    default: -1
  })
  parser.add_argument('--concurrency', {
    required: false,
    help: 'Number of concurrent file changes per thread',
    default: 32
  })
  parser.add_argument('--log-level', {
    required: false,
    help: 'Log Level',
    default: 1
  })

  parser.add_argument('--batch-file', {
    required: false,
    help: 'File Containing Users',
    default: null
  })

  parser.add_argument('--validate', {
    required: false,
    action: "store_true",
    help: 'PreFlight Check'
  })

  parser.add_argument('--populate-groups', {
    required: false,
    action: "store_true",
    help: 'Populate Run-As-Root group'
  })

  return parser.parse_args()
}

(async () => {
  const { hostname, path, threads, concurrency, log_level, batch_file, validate, populate_groups } = GetArguments()
  const { username, password } = await GetCredentials()

  let workers = [];
  let workers2 = [];
  let users = [];
  let staged_users = [];

  repairPath = path;

  const isilon = new IsilonClient({
    ssip: hostname,
    username: username,
    password: password
  })
  const axios = isilon.ssip.axios

  GetFolderRedirectsFromFile = async (file) => {
    const data = fs.readFileSync(file);
    let results = [];

    let user_lookup_table = await NormalizeUserList(path);

    console.log(`Adding FolderRedirects from ${file}`)


    for (line of data.toString().trim().split('\r\n')) {
      let username = user_lookup_table[line.toLowerCase()];

      if (username) {
        try {
          console.log(`+ ${path}/${username} [${line}]`);
          results.push(`${path}/${username}`);
        } catch (error) {
          throw error;
        }
      } else {
        console.log(`- ${path}/${line} -- [Path Not Found]`)
      }
    }

    return results;
  }

  GetFolderRedirects = async (path_s) => {
    const results = []
    console.log(`Adding FolderRedirects from ${path_s}`)
    const path = isilon.namespace.get(path_s)


    for (const result of await path.readdir()) {

      if (result && result.type === 'container') {
        console.log(`+ ${result.path}`)
        results.push(result.path)
      }
    }

    return results
  }

  WorkerCallback = async (payload) => {
    if (payload.msg === 'update_results') {
      let results = payload.results;
      let deltaTime = ((results.completedAt - results.startedAt) / 1000).toFixed(2)

      report.jobs.push(results)

      iops = 0;
      try {
        for (key of Object.keys(results.stats)) {
          iops += results.stats[key];
          report.stats[key] += results.stats[key];
        }
        iops = (iops / deltaTime).toFixed(0);

        let fileStats = `${results.stats.filesFixed}/${results.stats.filesScanned} (${(results.stats.filesFixed / results.stats.filesScanned) * 100}%)`
        let dirStats = `${results.stats.dirsFixed}/${results.stats.dirsScanned} (${(results.stats.dirsFixed / results.stats.dirsScanned) * 100}%)`

        let status = payload.shutdown === true ? chalk.red('INTERRUPTED') : chalk.green('COMPLETED');
        results.status = status;

        logger(`${status} ${results.path} [Files:  ${fileStats} | Dirs:  ${dirStats} | Time:  ${deltaTime}s | IO/s: ${iops}]`)
      } catch (error) {
      }

      if (payload.shutdown === true) {
        workers[payload.id].postMessage({ cmd: 'shutdown' })
      }
    }

    if (payload.msg == 'repair_permissions') {
      staged_users[payload.uname]--;

      if (staged_users[payload.uname] === 0) {
        workers[payload.id].postMessage({ cmd: 'repair_permissions', path: payload.folderRedirect, user: users[payload.uname], uname: payload.uname })
      }
    }

    if (payload.msg === 'close_open_files') {
      nextPath = folderRedirects.pop();

      if (nextPath) {
        uname = nextPath.substring(nextPath.lastIndexOf('/') + 1).toLowerCase()

        staged_users[uname] = 0;

        logger(`CLOSING OPEN FILES AND SESSIONS FOR ${uname}`);
        for (worker of workers2) {
          staged_users[uname]++;
          worker.postMessage({ cmd: 'close_open_files', path: path, folderRedirect: nextPath, user: users[uname], uname: uname, id: payload.id })
        }
      } else {
        logger(`${payload.name} COMPLETE, SHUTTING DOWN.`)

        workers[payload.id].removeListener('message', WorkerCallback)
        workers[payload.id].unref()
        numThreads--;

        if (numThreads == 0) {
          for (worker of workers2) {
            worker.postMessage({ cmd: 'shutdown' })
          }
          printFinalReport();
        }
      }
    }
  }

  CreateWorker = (config, id) => {
    const name = 'THREAD' + (id + 1).toString().padStart(3, '0')
    const worker = new Worker(`${__dirname}/worker.js`, {
      workerData: { config, name, id, concurrency, log_level }
    })

    worker.on('error', (err) => {
      throw err
    })
    worker.on('message', WorkerCallback)

    return worker
  }

  CreateFileWorker = (config, id) => {
    const name = 'SESSION' + (id + 1).toString().padStart(2, '0')
    const worker = new Worker(`${__dirname}/worker2.js`, {
      workerData: { config, name, id, concurrency, log_level }
    })

    worker.on('error', (err) => {
      throw err
    })
    worker.on('message', WorkerCallback)

    return worker
  }

  GetAccessZoneForPath = async (path) => {
    let zone = null
    const response = await axios.get('/platform/11/zones')

    for (const z of response.data.zones) {
      if (path.startsWith(z.path)) {
        zone = z
      }
    }

    return zone
  }

  GetUser = async ({ provider, username, axios }) => {
    const provider_id = provider.split(':')[1]
    const url = `/platform/6.1/auth/providers/ads/${provider_id}/search`

    const payload = {
      search_users: true,
      limit: 1,
      filter: username
    }

    try {
      const response = await axios.post(url, payload)
      if (response.data.objects.length == 1) {
        return response.data.objects[0]
      } else {
        return null
      }
    } catch (error) {
      throw error;
    }
  }

  GetUsers = async ({ provider, resume = null }) => {
    let user = null
    let users = []
    const results = {}
    const provider_id = provider.split(':')[1]

    const url = `/platform/6.1/auth/providers/ads/${provider_id}/search`

    const payload = {
      search_users: true,
      limit: 1000
    }

    if (resume) {
      payload.resume = resume
    }

    try {
      const response = await axios.post(url, payload)

      users = users.concat(response.data.objects)

      if (response.data.resume) {
        console.log("Fetching next 1000 users");
        users = users.concat(
          await GetUsers({ provider: provider, resume: response.data.resume })
        )
      }
    } catch (error) {
      throw error
    }

    users.map((u) => {
      u.id.username = u.id.name.split('\\')[1]
    })

    for (user of users) {
      results[user.id.username.toLowerCase()] = user
    }

    return results
  }

  GetPool = async ({ groupnet, subnet, pool }) => {
    const url = `/platform/11/network/groupnets/${groupnet}/subnets/${subnet}/pools/${pool}`

    const response = await axios.get(url)

    return response.data.pools[0]
  }

  GetSubnet = async ({ groupnet, subnet }) => {
    const url = `/platform/11/network/groupnets/${groupnet}/subnets/${subnet}`

    const response = await axios.get(url)

    return response.data.subnets[0]
  }

  GetAccessZonePool = async (zone) => {
    let subnets = []
    let pools = []
    const url = `/platform/11/network/groupnets/${zone.groupnet}`

    const response = await axios.get(url)
    const groupnet = response.data.groupnets[0]

    for (_subnet of groupnet.subnets) {
      const subnet = await GetSubnet({
        groupnet: groupnet.name,
        subnet: _subnet
      })
      subnets = subnets.concat(_subnet)

      for (_pool of subnet.pools) {
        const pool = await GetPool({
          groupnet: groupnet.name,
          subnet: _subnet,
          pool: _pool
        })
        pools = pools.concat(pool)
      }
    }

    return pools.filter((pool) => {
      return pool.access_zone === zone.name
    })[0]
  }

  const GetGroup = async ({ group, zone }) => {
    let baseUrl = `/platform/11/auth/groups/${group}?zone=${zone}`

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

  const AddUserToGroup = async ({ user, group, zone, axios }) => {
    let baseUrl = `/platform/11/auth/groups/${group}/members?zone=${zone.name}`
    let g;

    // try {
    //   g = await GetGroup({ group: group, zone: zone.name });
    // } catch (error) {
    //   console.log(error);
    // }
    // if (g === undefined) {
    //   return undefined
    // }

    const payload = {
      type: "user",
      id: user.id.id
    }

    try {
      let response = await axios.post(baseUrl, payload);
      console.log(`++> Added ${u.id.name} to '${group}'`);
      return true;
    } catch (error) {
      return false;
    }
  }

  // const PopulateRootUsersGroup = async ({ zone, users }) => {
  //   const results = []

  //   for (user of Object.keys(users)) {
  //     let u = users[user]
  //     console.log(`=> Adding ${u.id.name} to 'Run-As-Root'`);
  //     await AddUserToGroup({ user: u, group: 'Run-As-Root', zone: zone });
  //   }

  //   return results
  // }

  NormalizeUserList = async (path) => {
    let users = [];

    console.log(`Normalizing Usernames from ${path}, this may take some time...`);
    let folder = await isilon.namespace.get(path);

    for (child of await folder.readdir()) {
      users[child.name.toLowerCase()] = child.name;
    }

    return users;
  }

  GetNodeIPs = async (pool) => {
    const url = '/platform/11/network/interfaces'
    let response
    try {
      response = await axios.get(url)
    } catch (error) {
      throw error
    }

    const interfaces = response.data.interfaces

    // Danger Will Robinson, fancy filtering going on here.  Is this
    // efficient?  I don't know but it works!
    const nodes = interfaces
      .filter((i) => {
        // Remove any downed interfaces or interfaces with no IP assignments
        return i.ip_addrs.length > 0 && i.status === 'up'
      })
      .map((i) => {
        // Create a new object that has only lnn and ip_addrs field.  The
        // ip_addrs field is populated from the first ip address from the
        // owners which are members of groupnet/subnet/pool
        return {
          lnn: i.lnn,
          ip_addrs: i.owners
            .filter((o) => {
              return (
                o.groupnet === pool.groupnet &&
                o.pool === pool.name &&
                o.ip_addrs.length > 0
              )
            })
            .map((o) => o.ip_addrs[0]) // this
        }
      })
      .filter((i) => {
        // Remove any objects that have no ip_addrs
        return i.ip_addrs.length > 0
      })
      .map((i) => {
        // Create final object with lnn and ip field with single ip
        return {
          lnn: i.lnn,
          ip: i.ip_addrs[0]
        }
      })

    // It is possible that an lnn will have multiple ip assignments when
    // not using LACP.  Filter out additional ip assigments return only
    // one ip assigment per lnn.
    return [...new Map(nodes.map((item) => [item.lnn, item])).values()].map(
      (i) => {
        return i.ip
      }
    )
  }

  // Get Access Zone for path and auto provider for SID lookups
  // let zone = await GetAccessZoneForPath("/ifs/edith/data1/shares/vol1/FolderRedirect");
  const zone = await GetAccessZoneForPath(path)
  const provider = zone.auth_providers.filter((provider) => {
    return provider.startsWith('lsa-activedirectory-provider:')
  })[0]
  console.log(`Access Zone:  ${zone.name}`);
  console.log(`AD Provider:  ${provider}`)

  // Build a list of Node IPs
  const pool = await GetAccessZonePool(zone)
  const nodes = await GetNodeIPs(pool)
  numThreads = threads == -1 ? nodes.length : threads
  console.log(`Hostname: ${pool.sc_dns_zone}`)
  console.log(`Nodes IPs: ` + nodes.join(", "))
  console.log(`Threads:  ${numThreads}`)

  console.log('Establishing Node API Sessions')
  for (node of nodes) {
    PAPI_SESSIONS.push(
      new IsilonClient({
        ssip: node,
        username: username,
        password: password
      })
    )
  }

  const PopulateRootUsersQueue = async.queue(async ({ user, group, zone, axios }, callback) => {
    try {
      await AddUserToGroup({ user, group, zone, axios });
    } catch (error) {
      console.log(error);
    }
  }, PAPI_SESSIONS.length);

  const UserQueue = async.queue(async ({ username, provider, axios }, callback) => {
    try {
      let t = await GetUser({ username: username, provider: provider, axios: axios });

      if (t) {
        users[username.toLowerCase()] = t
        console.log(`=> ${username} [${t.id.id}][${t.id.name}]`)
      } else {
        console.log(`-> ${username} does not have an AD account.`)
      }
    } catch (error) {
      throw error;
    }
  }, concurrency);

  // Build a list of all AD users.  This should be more efficient than making
  // multiple API calls for individual users when running against large sets
  // of users.  This method works very well when API endpoint is slow.
  // console.log('Prefetching Active Directory Users, this may take some time.')
  // const users = await GetUsers({ provider: provider })

  if (batch_file && fs.existsSync(batch_file)) {
    folderRedirects = await GetFolderRedirectsFromFile(batch_file);
  } else if (batch_file && !fs.existsSync(batch_file)) {
    console.log(`ERROR:  ${batch_file} does not exist!`)
  } else {
    folderRedirects = await GetFolderRedirects(path)
  }

  // Build a list of all AD users.  This should be more efficient than making
  // multiple API calls for individual users when running against large sets
  // of users.  This method works very well when API endpoint is slow.
  console.log('Prefetching Active Directory Users, this may take some time.')
  for ([i, u] of folderRedirects.map((f) => f.split("/").at(-1)).entries()) {
    let axios = PAPI_SESSIONS[i % PAPI_SESSIONS.length].ssip.axios;
    UserQueue.push({ username: u, provider: provider, axios: axios })
  }
  await UserQueue.drain();

  if (populate_groups === true) {
    console.log("POPULATING GROUPS");
    // await PopulateRootUsersGroup({ zone: zone.name, users });

    for ([i, user] of Object.keys(users).entries()) {
      let u = users[user]
      console.log(`=> Adding ${u.id.name} to 'Run-As-Root'`);
      let axios = PAPI_SESSIONS[i % PAPI_SESSIONS.length].ssip.axios;
      PopulateRootUsersQueue.push({ user: u, group: 'Run-As-Root', zone: zone, axios: axios })
      // await AddUserToGroup({ user: u, group: 'Run-As-Root', zone: zone });
    }
    await PopulateRootUsersQueue.drain();
    process.exit();
  }

  if (validate) {
    process.exit();
  }

  process.on('SIGINT', async function () {
    for (worker of workers) {
      worker.postMessage({ cmd: 'interrupt' })
    }

    for (worker of workers2) {
      worker.postMessage({ cmd: 'shutdown' })
    }

    await sleep(1000);

    printFinalReport();
  });

  console.log(
    '======================================================================================='
  )

  for (let i = 0; i < nodes.length; i++) {
    config = {
      ssip: nodes[i % nodes.length],
      username: username,
      password: password
    }
    workers2.push(CreateFileWorker(config, i));
    await sleep(100);
  }

  await sleep(1000);

  for (let i = 0; i < numThreads; i++) {
    config = {
      ssip: nodes[i % nodes.length],
      username: username,
      password: password
    }
    workers.push(CreateWorker(config, i));
    await sleep(100);
  }

  await sleep(1000);

  for (let i = 0; i < numThreads; i++) {
    config = {
      ssip: nodes[i % nodes.length],
      username: username,
      password: password
    }
    workers[i].postMessage({ cmd: 'start_processing' });
  }
})();