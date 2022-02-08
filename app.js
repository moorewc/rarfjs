const { Worker } = require('worker_threads')
const { ArgumentParser } = require('argparse')
const { exit } = require('process')
const fs = require('fs')
const lineReader = require('line-reader');

const prompts = require('prompts')
const IsilonClient = require('@moorewc/node-isilon')
// const { parse } = require('path')
// const { fileURLToPath } = require('url')
// const { fstat } = require('fs')
const async = require('async')
const chalk = require('chalk')



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

  return parser.parse_args()
}

(async () => {
  const { hostname, path, threads, concurrency, log_level, batch_file, validate } = GetArguments()
  const { username, password } = await GetCredentials()

  let workers = [];
  let users = []

  const isilon = new IsilonClient({
    ssip: hostname,
    username: username,
    password: password
  })
  const axios = isilon.ssip.axios

  const UserQueue = async.queue(async ({ username, provider }, callback) => {
    try {
      let t = await GetUser({ username: username, provider: provider });

      if (t) {
        users[username.toLowerCase()] = t
        console.log(`=> ${username} [${t.id.id}] [${t.id.name}]`)
      }
    } catch (error) {
      throw error;
    }
  }, concurrency)

  GetFolderRedirectsFromFile = async (file) => {
    const data = fs.readFileSync(file);
    let results = [];

    console.log(`Adding FolderRedirects from ${file}`)

    for (line of data.toString().split('\r\n')) {
      if (line) {

        try {
          const _path = await isilon.namespace.get(`${path}/${line}`)
          let exists = await _path.exists();
          if (exists) {
            console.log(`+ ${_path.path}`)
            results.push(_path.path);
          } else {
            console.log(`- ${_path.path} [Path Not Found]`)
          }
        } catch (error) {
          throw error;
        }
      }
    }

    return results;
  }

  GetFolderRedirects = async (path_s) => {
    const results = []
    console.log(`Adding FolderRedirects from ${path_s}`)
    const path = isilon.namespace.get(path_s)


    for (const result of await path.readdir()) {
      if (result) {
        console.log(`+ ${result.path}`)
        results.push(result.path)
      }
    }

    return results
  }

  WorkerCallback = async (payload) => {
    if (payload.msg === 'next') {
      nextPath = folderRedirects.pop()

      if (nextPath) {
        uname = nextPath.substring(nextPath.lastIndexOf('/') + 1).toLowerCase()
        workers[payload.id].postMessage({ path: nextPath, user: users[uname] })
      } else {
        console.log('[' + chalk.blue(payload.name) + `] THREAD COMPLETE, CLOSING.`)

        workers[payload.id].removeListener('message', WorkerCallback)
        workers[payload.id].unref()
        numThreads--;
        if (numThreads == 0) {
          console.log(
            '======================================================================================='
          )
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

  GetUser = async ({ provider, username }) => {
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
  for (u of folderRedirects.map((f) => f.split("/").at(-1))) {
    UserQueue.push({ username: u, provider: provider })
  }
  await UserQueue.drain();

  if (validate) {
    process.exit();
  }
  require('log-timestamp')
  console.log(
    '======================================================================================='
  )

  for (let i = 0; i < numThreads; i++) {
    config = {
      ssip: nodes[i % nodes.length],
      username: username,
      password: password
    }

    workers.push(CreateWorker(config, i))
  }
})()
