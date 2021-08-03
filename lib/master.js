const os = require('os'),
    cluster = require('cluster'),
    ClusterProcess = require('./cluster_process'),
    WorkerWrapper = require('./worker_wrapper'),
    Port = require('./port'),
    RestartQueue = require('./restart_queue'),
    RPC = require('./rpc');

const net = require('net');
const fs = require('fs');
const path = require('path');
let EXP = process.env.ANSEAL_B
if( EXP ) {
    console.log('EXP:', EXP)
    EXP = new Set(EXP.split("."))
    // rr / wait / wait.wait_rr / wait.wait_rr.use_idle / wait.wait_rr.use_idle.idle_rr / wait.wait_rr.use_idle.idle_rr.r_tail
    // pause
    // wait_rr
    // use_idle
    // write_log
} else {
    console.log('EXP: none', EXP)
}

/**
 * @constructor
 * @class Master
 * @augments ClusterProcess
 */
let id = 0
class Master extends ClusterProcess {
    constructor() {
        super();

        if( EXP ) {
            this.wi = undefined // anseal
            this.rr = 0
            this.queue = []
            this.log = []
            this.connections = new Map()
            this.ci = 0
            this.profiler_i = 0
            this.server = net.createServer(
               {pauseOnConnect: !EXP.has('read_headers')},
             (connection) => {
                const workers = this.getWorkersArray()
                for( let i = 0 ; i !== workers.length ; ++i) {
                    if( !workers[i].weights) workers[i].weights = []
                }
                const data = undefined
                if( EXP.has('read_headers')) {

                    const onData = (data) => {
                        connection.pause()
                        connection.removeListener('data', onData)

                        // workers[Math.round(Math.random() * (workers.length-1))].send({ msg: "doit", data, weight: 0 }, connection)
                        // return
                        // if( ! this.wi ) {
                        //     this.wi = workers.length
                        // }
                        // --this.wi
                        // workers[this.wi].send({ msg: "doit", data, weight: 0 }, connection)
                        // return

                        const str = data.toString()
                        let weight
                        const request_str = str.slice(0,str.indexOf('\n'))
                        if( request_str.includes('/list?') ) { //|| str.includes('/search?') )
                            weight = 6000
                        } else {
                            if( request_str.includes('/profiler?') ) {
                                if( ! this.profiler_i ) {
                                    this.profiler_i = workers.length
                                }
                                --this.profiler_i
                                workers[this.profiler_i].send({ msg: "doit", data, weight: 0 }, connection)
                                return
                            }
                            weight = 1000
                        }
                        let i = 0
                        let min_weight = Infinity
                        let min_i = i
                        const time = Date.now()
                        for( ; i !== workers.length ; ++i) {
                            // const ww = workers[i].weights.reduce((sum, w) => sum + w.w - (time - w.t), 0)
                            const ww = workers[i].weight || 0
                            if( min_weight > ww ) {
                                min_weight = ww
                                min_i = i
                            }
                        }
                        const worker = workers[min_i]
                        worker.send({ msg: "doit", data, weight }, connection)    
                        workers.push(workers.splice(min_i,1)[0])
                        // worker.weights.push({w: weight, t: Date.now(), id: ++id})
                        worker.weight = (worker.weight || 0 )+ weight
                        worker.tt = Date.now()
                        // console.log(worker.weight)
                    }
                    connection.on('data', onData)
                    return
                }

                if( EXP.has('rr') ) {
                    if( ! this.wi ) {
                        this.wi = workers.length
                    }
                    --this.wi
                    workers[this.wi].send({ msg: "doit", data }, connection)
                } else {
                    if( this.wi === undefined ) {
                        this.wi = workers.length
                    }
                    if( this.wi !== 0 ) {
                        --this.wi
                        workers[this.wi].send({ msg: "doit", data }, connection)
                        return
                    }

                    this.queue.push(connection)
                    if( EXP.has('write_log')) {
                        if( this.connections.has(connection) ) {
                            console.log('!!!!!!!!!!!!!!!!!!!!!! reused connection')
                        }
                        this.connections.set(connection, this.ci++)
                        this.log_record('new_connection')
                    }

                    // TODO: random of 2?
                    let i = 0
                    let len_left = workers.length
                    for( ; i !== len_left ; ++i) {
                        if( ! workers[i].requests_in_progress ) {
                            const connection = this.queue.shift()
            
                            workers[i].requests_in_progress++
                            workers[i].is_idle = false
                            workers[i].send({ msg: "doit", data }, connection)
                            if( EXP.has('write_log')) {
                                this.log_record('dispatch connection on arrival'  + workers[i].wid)
                            }
                            if( EXP.has('wait_rr') ) {
                                workers.push(workers.splice(i,1)[0])
                                --len_left
                                --i
                            }
                            if( this.queue.length === 0 ) {
                                break
                            }
                        }
                    }
                    if( EXP.has('use_idle') ) {
                        if( this.queue.length !== 0 ) {
                            if( i === len_left ) {
                                len_left = workers.length
                                for( let i = 0 ; i !== len_left ; ++i) {
                                    if( workers[i].is_idle) {
                                    // if( workers[i].is_idle && workers[i].requests_in_progress === 1) {
                                        const connection = this.queue.shift()

                                        workers[i].requests_in_progress++
                                        workers[i].is_idle = false
                                        workers[i].send({ msg: "doit", data }, connection)
                                        if( EXP.has('idle_rr') ) {
                                            workers.push(workers.splice(i,1)[0])
                                            --len_left
                                            --i
                                        }
                                        if( this.queue.length === 0 ) {
                                            break
                                        }
                                    }
                                } 
                            }
                        }
                    }
                    if( EXP.has('r_tail')) {
                        while( this.queue.length !== 0 ) {
                            const connection = this.queue.shift()
                            let i = Math.round(Math.random() * (workers.length-1))
                            workers[i].requests_in_progress++
                            workers[i].is_idle = false
                            workers[i].send({ msg: "doit", data }, connection)
                        }
                    }
                    if( EXP.has('rr_tail')) {
                        while( this.queue.length !== 0 ) {
                            const connection = this.queue.shift()
                            let i = 0
                            workers[i].requests_in_progress++
                            workers[i].is_idle = false
                            workers[i].send({ msg: "doit", data }, connection)
                            workers.push(workers.splice(i,1)[0])
                        }
                    }
                }
            })
            this.server
                .on('error', (err) => console.log("!!!!!1234 not listening", err))
                .listen('server.sock', () => console.log("!!!!!!!!!! listening"));
            if( EXP.has('write_log')) {
                setTimeout(() => {
                    console.log("!!!!!!!!!! write log")
                    const profilePath = global.process.env.LOGS_DIR || global.process.cwd;
                    const filename = path.join(profilePath, `balancer.json.cpuprofile`);
                    fs.writeFile(filename, JSON.stringify(this.log, undefined, '\t'), err => {
                        if (err) {
                        console.log(err);
                        return;
                        }
                
                        console.log('Successfully written Balancer profile.');
                    });
                }, 160000)
            }
        }
        /**
         * @type {Object}
         * @property {WorkerWrapper} *
         * @public
         * @todo make it private or public immutable
         */
        this.workers = {};

        /**
         * Workers restart queue.
         * @type {RestartQueue}
         * @private
         */
        this._restartQueue = new RestartQueue();

        /**
         * Configuration object to pass to cluster.setupMaster()
         * @type {Object}
         * @private
         */
        this._masterOpts = {};

        this.id = 0;
        this.wid = 0;
        this.pid = process.pid;

        this.on('worker state', this._cleanupUnixSockets.bind(this));
        this.on('worker exit', this._checkWorkersAlive.bind(this));

        // @todo make it optional?
        process.on('SIGINT', this._onSignalQuit.bind(this));
        process.on('SIGQUIT', this._onSignalQuit.bind(this));
    }

    /**
     * Allows same object structure as cluster.setupMaster().
     * This function must be used instead of cluster.setupMaster(),
     * because all calls of cluster.setupMaster() ignored, except first one.
     * An instance of Master will call it, when running.
     * @param {Object} opts
     * @see {@link http://nodejs.org/api/cluster.html#cluster_cluster_setupmaster_settings}
     */
    setup(opts) {
        Object.assign(this._masterOpts, opts);
    }

    /**
     * SIGINT and SIGQUIT handler
     * @private
     */
    _onSignalQuit() {
        this
            .once('shutdown', () => process.exit(0))
            .shutdown();
    }

    /**
     * Remove not used unix socket before worker will try to listen it.
     * @param {WorkerWrapper} worker
     * @param {WorkerWrapperState} state
     * @private
     */
    _cleanupUnixSockets(worker, state) {
        const port = worker.options.port;

        if (this._restartQueue.has(worker) ||
            state !== WorkerWrapper.STATES.LAUNCHING ||
            port.family !== Port.UNIX) {
            return;
        }

        const inUse = this.getWorkersArray().some(w =>
            worker.wid !== w.wid &&
            w.isRunning() &&
            port.isEqualTo(w.options.port)
        );

        if (!inUse) {
            port.unlink(err => {
                if (err) {
                    this.emit('error', err);
                }
            });
        }
    }

    /**
     * Check for alive workers, if no one here, then emit "shutdown".
     * @private
     */
    _checkWorkersAlive() {
        const workers = this.getWorkersArray(),
            alive = workers.reduce(
                (count, w) => w.dead ? count - 1 : count,
                workers.length
            );

        if (alive === 0) {
            this.emit('shutdown');
        }
    }

    /**
     * Repeat WorkerWrapper events on Master and add 'worker ' prefix to event names
     * so for example 'online' became 'worker online'
     * @private
     * @param {WorkerWrapper} worker
     */
    _proxyWorkerEvents(worker) {
        WorkerWrapper.EVENTS
            .forEach(eventName => {
                const proxyEventName = 'worker ' + eventName;
                worker.on(eventName, this.emit.bind(this, proxyEventName, worker));
            });

        if( EXP ) {
            worker.on('message', (message) => {
                // console.log("M", message)
                if( message && message.msg === 'real_request_closed') {
                    // console.log(worker.weight)
                    // const index = worker.weights.findIndex(w=> w.id ===message.id)
                    // if( index !== -1 ) {
                    //     worker.weights.splice(index, 1)
                    // }
                    worker.weight -= message.weight
                    // console.log(worker.weight, Date.now() - worker.tt)
                //     if( EXP.has('write_log')) {
                //         this.log_record('real request_closed' + worker.wid)
                //     }
                // } else if( message === 'request_closed') {
                    worker.requests_in_progress -= 1
                    // console.log(message, worker.wid, worker.requests_in_progress)
                    // console.log(message, worker.requests_in_progress) // однажды видел здесь NaN, так и не понял почему
                    
                    if( EXP.has('write_log')) {
                        this.log_record('request_closed' + worker.wid)
                    }
                    if( ! worker.requests_in_progress ) {
                        if( this.queue.length !== 0 ) {
                            const connection = this.queue.shift()
                            worker.requests_in_progress++
                            worker.is_idle = false
                            worker.send({ msg: "doit", data }, connection)
                            if( EXP.has('write_log')) {
                                this.log_record('dispatch connection on request_closed' + worker.wid)
                            }
                        }
                    }
                } else if( message === 'idle' ) {
                    if( EXP.has('use_idle') ) {
                        worker.is_idle = true
                        // if( worker.requests_in_progress === 1) {
                            if( this.queue.length !== 0 ) {
                                const connection = this.queue.shift()
                                worker.requests_in_progress++
                                worker.is_idle = false
                                worker.send({ msg: "doit", data }, connection)
                                if( EXP.has('write_log')) {
                                    this.log_record('dispatch in idle on request_closed' + worker.wid)
                                }
                            }
                        // }
                    }
                } else if( message === 'busy' ) {
                    worker.is_idle = false
                }
            })
        }
    }
    log_record(event) {
        const workers = this.getWorkersArray()
        this.log.push({
            event,
            time: Date.now(),
            queue: this.queue.map(connection => this.connections.get(connection)),
            workers: workers.map(worker => ({
                wid: worker.wid,
                requests_in_progress: worker.requests_in_progress,
                is_idle: worker.is_idle,
            }))
        })
    }

    /**
     * @returns {number[]} workers ids array
     */
    getWorkersIds() {
        if (!this._workersIdsCache) {
            this._workersIdsCache = this.getWorkersArray().map(w => w.wid);
        }

        return this._workersIdsCache;
    }

    /**
     * @returns {WorkerWrapper[]} workers array
     */
    getWorkersArray() {
        if (!this._workersArrayCache) {
            this._workersArrayCache = Object.values(this.workers);
        }

        return this._workersArrayCache;
    }

    /**
     * Add worker to the pool
     * @param {WorkerWrapper} worker
     * @returns {Master} self
     * @public
     */
    add(worker) {
        // invalidate Master#getWorkersIds and Master#getWorkersArray cache
        this._workersIdsCache = null;
        this._workersArrayCache = null;

        this.workers[worker.wid] = worker;
        this._proxyWorkerEvents(worker);

        return this;
    }

    /**
     * Iterate over workers in the pool.
     * @param {Function} fn
     * @public
     * @returns {Master} self
     *
     * @description Shortcut for:
     *      master.getWorkersArray().forEach(fn);
     */
    forEach(fn) {
        this.getWorkersArray()
            .forEach(fn);

        return this;
    }

    /**
     * Broadcast an event received by IPC from worker as 'worker <event>'.
     * @param {WorkerWrapper} worker
     * @param {String} event
     * @param {...*} args
     */
    broadcastWorkerEvent(worker, event, ...args) {
        this.emit('received worker ' + event, worker, ...args);
    }

    /**
     * Configure cluster
     * @override ClusterProcess
     * @private
     */
    _onConfigured() {
        super._onConfigured();

        // register global remote command in the context of master to receive events from master
        if (!this.hasRegisteredRemoteCommand(RPC.fns.master.broadcastWorkerEvent)) {
            this.registerRemoteCommand(
                RPC.fns.master.broadcastWorkerEvent,
                this.broadcastWorkerEvent.bind(this)
            );
        }

        const // WorkerWrapper options
            forkTimeout = this.config.get('control.forkTimeout'),
            stopTimeout = this.config.get('control.stopTimeout'),
            exitThreshold = this.config.get('control.exitThreshold'),
            allowedSequentialDeaths = this.config.get('control.allowedSequentialDeaths'),

            count = this.config.get('workers', os.cpus().length),
            isServerPortSet = this.config.has('server.port'),
            groups = this.config.get('server.groups', 1),
            workersPerGroup = Math.floor(count / groups);

        let port,
            // workers and groups count
            i = 0,
            group = 0,
            workersInGroup = 0;

        if (isServerPortSet) {
            port = new Port(this.config.get('server.port'));
        }

        // create pool of workers
        while (count > i++) {
            this.add(new WorkerWrapper(this, {
                forkTimeout,
                stopTimeout,
                exitThreshold,
                allowedSequentialDeaths,
                port: isServerPortSet ? port.next(group) : 0,
                maxListeners: this.getMaxListeners(),
            }));

            // groups > 1, current group is full and
            // last workers can form at least more one group
            if (groups > 1 &&
                ++workersInGroup >= workersPerGroup &&
                count - (group + 1) * workersPerGroup >= workersPerGroup) {
                workersInGroup = 0;
                group++;
            }
        }
    }

    /**
     * @param {Number[]} wids Array of `WorkerWrapper#wid` values
     * @param {String} event wait for
     * @public
     * @returns {Promise<void>}
     */
    waitForWorkers(wids, event) {
        const pendingWids = new Set(wids);

        return new Promise(resolve => {
            if (pendingWids.size === 0) {
                resolve();
            }

            const onWorkerState = worker => {
                const wid = worker.wid;
                pendingWids.delete(wid);
                if (pendingWids.size === 0) {
                    this.removeListener(event, onWorkerState);
                    resolve();
                }
            };
            this.on(event, onWorkerState);
        });
    }

    /**
     * @param {String} event wait for
     * @public
     * @returns {Promise<void>}
     */
    waitForAllWorkers(event) {
        return this.waitForWorkers(
            this.getWorkersIds(),
            event
        );
    }

    /**
     * @event Master#running
     */

    /**
     * @event Master#restarted
     */

    async _restart() {
        // TODO maybe run this after starting waitForAllWorkers
        this.forEach(worker => worker.restart());

        await this.waitForAllWorkers('worker ready');

        this.emit('restarted');
    }

    /**
     * Hard workers restart: all workers will be restarted at same time.
     * CAUTION: if dead worker is restarted, it will emit 'error' event.
     * @public
     * @returns {Master} self
     * @fires Master#restarted when workers spawned and ready.
     */
    restart() {
        this._restart();
        return this;
    }

    /**
     * Workers will be restarted one by one using RestartQueue.
     * If a worker becomes dead, it will be just removed from restart queue. However, if already dead worker is pushed
     * into the queue, it will emit 'error' on restart.
     * @public
     * @returns {Master} self
     * @fires Master#restarted when workers spawned and ready.
     */
    softRestart() {
        this.forEach(worker => worker.softRestart());
        this._restartQueue.once('drain', this.emit.bind(this, 'restarted'));
        return this;
    }

    /**
     * Schedules one worker restart using RestartQueue.
     * If a worker becomes dead, it will be just removed from restart queue. However, if already dead worker is pushed
     * into the queue, it will emit 'error' on restart.
     * @public
     * @param {WorkerWrapper} worker
     * @returns {Master} self
     */
    scheduleWorkerRestart(worker) {
        this._restartQueue.push(worker);
        return this;
    }

    /**
     * @override
     * @see ClusterProcess
     * @private
     */
    _setupIPCMessagesHandler() {
        this.on('worker message', this._onMessage.bind(this));
    }

    /**
     * RPC to all workers
     * @method
     * @param {String} name of called command in the worker
     * @param {...*} args
     * @public
     */
    remoteCallToAll(name, ...args) {
        this.forEach(worker => {
            if (worker.ready) {
                worker.remoteCall(name, ...args);
            } else {
                worker.on('ready', () => {
                    worker.remoteCall(name, ...args);
                });
            }
        });
    }

    /**
     * Broadcast event to all workers.
     * @method
     * @param {String} event of called command in the worker
     * @param {...*} args
     * @public
     */
    broadcastEventToAll(event, ...args) {
        this.forEach(worker => {
            if (worker.ready) {
                worker.broadcastEvent(event, ...args);
            }
        });
    }

    /**
     * Emit event on master and all workers in "ready" state.
     * @method
     * @param {String} event of called command in the worker
     * @param {...*} args
     * @public
     */
    emitToAll(event, ...args) {
        this.emit(event, ...args);
        this.broadcastEventToAll(event, ...args);
    }

    /**
     * @event Master#shutdown
     */

    async _shutdown() {
        const stoppedWorkers = [];

        this.forEach(worker => {
            if (worker.isRunning()) {
                worker.stop();
                stoppedWorkers.push(worker.wid);
            }
        });

        await this.waitForWorkers(
            stoppedWorkers,
            'worker exit',
        );

        this.emit('shutdown');
    }

    /**
     * Stop all workers and emit `Master#shutdown` event after successful shutdown of all workers.
     * @fires Master#shutdown
     * @returns {Master}
     */
    shutdown() {
        this._shutdown();
        return this;
    }

    /**
     * Do a remote call to all workers, callbacks are registered and then executed separately for each worker
     * @method
     * @param {String} opts.command
     * @param {Function} opts.callback
     * @param {Number} [opts.timeout] in milliseconds
     * @param {*} [opts.data]
     * @public
     */
    remoteCallToAllWithCallback(opts) {
        this.forEach(worker => {
            if (worker.isRunning()) {
                worker.remoteCallWithCallback(opts);
            }
        });
    }

    async _run() {
        await this.whenInitialized();

        cluster.setupMaster(this._masterOpts);

        // TODO maybe run this after starting waitForAllWorkers
        this.forEach(worker => worker.run());

        await this.waitForAllWorkers('worker ready');

        this.emit('running');
    }

    /**
     * Fork workers.
     * Execution will be delayed until Master became configured
     * (`configured` event fired).
     * @method
     * @returns {Master} self
     * @public
     * @fires Master#running then workers spawned and ready.
     *
     * @example
     *      // file: master.js
     *      var master = require('luster');
     *
     *      master
     *          .configure({ app : 'worker' })
     *          .run();
     *
     *      // there is master is still not running anyway
     *      // it will run immediate once configured and
     *      // current thread execution done
     */
    run() {
        this._run();
        return this;
    }
}

module.exports = Master;
