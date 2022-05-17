const { workerData, parentPort } = require('worker_threads')
const IsilonClient = require('@moorewc/node-isilon')
const async = require('async')
const chalk = require('chalk')
require('log-timestamp')

const { config, name, id, concurrency, log_level } = workerData
const isilon = new IsilonClient(config)
const axios = isilon.ssip.axios

function logger(string) {
    console.log('[' + chalk.cyan(name) + `] ${string}`)
}

logger(`CONNECTING TO ${config.ssip}`)

const FileCloseQueue = async.queue(async ({ file }, callback) => {
    const url = `/platform/11/protocols/smb/openfiles/${file.id}`;
    const response = await isilon.ssip.axios.delete(url);
    logger(`Closed ${file.file} for ${file.user}`)
}, 10);

const SessionCloseQueue = async.queue(async ({ session }, callback) => {
    const username = encodeURIComponent(session.user);
    const url = `/platform/11/protocols/smb/sessions/${session.computer}/${username}`
    const response = await isilon.ssip.axios.delete(url);
    logger(`CLOSED SESSION FOR ${session.user}`)
});

async function GetOpenFilesForShare({ path, user }) {
    const url = `/platform/11/protocols/smb/openfiles`;

    const response = await isilon.ssip.axios.get(url);

    let openfiles = response.data.openfiles.map((f) => {
        return {
            file: f.file.replaceAll('\\', '/').replace('C:', ''),
            id: f.id,
            user: f.user
        }
    }).filter((a) => {
        return a.file.startsWith(path)
            && user.id.name.toLowerCase().includes(a.user.toLowerCase())
    }).map((f) => {
        return {
            file: path,
            user: f.user,
            id: f.id
        }
    });

    return openfiles
}

async function closeOpenFiles({ path, user }) {
    let files = await GetOpenFilesForShare({ path, user })

    for (file of files) {
        FileCloseQueue.push({ file });
    }
}

async function getOpenSessions({ user }) {
    const url = `/platform/1/protocols/smb/sessions`;

    const response = await isilon.ssip.axios.get(url);

    return response.data.sessions;

}

async function closeOpenSessions({ user }) {
    let sessions = await getOpenSessions({ user });

    for (session of sessions) {
        SessionCloseQueue.push({ session });
    }
}

parentPort.on('message', async ({ cmd, path, user }) => {
    if (cmd === 'close_files') {
        if (user) {
            await closeOpenFiles({ path, user })
            await closeOpenSessions({ user })
        }
    }

    if (cmd === 'shutdown') {
        logger("SHUTTING DOWN");
        process.exit();
    }
});