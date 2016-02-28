require('babel-register')({
  presets: [ 'es2015' ]
});
const app = require('express')();
const morgan = require('morgan')
const http = require('http').Server(app);
const uuid = require('node-uuid').v4;
const spawnteract = require('spawnteract');
const kernelspecs = require('kernelspecs');
const enchannel = require('enchannel-zmq-backend');
const fs = require('fs');
const io = require('socket.io')(http);
const logger = require('./logger');
const username = process.env.LOGNAME || process.env.USER ||
  process.env.LNAME || process.env.USERNAME;

const kernels = {}

function isChildMessage(msg) {
  return this.header.msg_id === msg.parent_header.msg_id;
}

app.use(morgan('combined'))

app.get('/spawn/*', function(req, res) {
  res.header('Access-Control-Allow-Origin', '*');

  const kernelName = req.url.split('/').slice(-1)[0];
  spawnteract.launch(kernelName).then(kernel => {
    const id = uuid();
    logger.kernelStarted(id, kernelName);

    var disconnectSockets;
    const disconnectedSockets = new Promise(resolve => disconnectSockets = resolve);

    console.log('crating new', kernelName, 'kernel with id', id)
    const kernelInfo = kernels[id] = {
      kernel,
      shell: enchannel.createShellSubject(id, kernel.config),
      stdin: enchannel.createStdinSubject(id, kernel.config),
      iopub: enchannel.createIOPubSubject(id, kernel.config),
      control: enchannel.createControlSubject(id, kernel.config),
      shellSocket: io.of('/shell/' + id),
      stdinSocket: io.of('/stdin/' + id),
      iopubSocket: io.of('/iopub/' + id),
      controlSocket: io.of('/control/' + id),
      disconnectSockets,
      createMessage(msg_type) {
        return {
          header: {
            username,
            id,
            msg_type,
            msg_id: uuid(),
            date: new Date(),
            version: '5.0',
          },
          metadata: {},
          parent_header: {},
          content: {},
        };
      }
    };

    // Connect sockets -> enchannel
    function connectSocketZmq(kernelSocket, name) {
      kernelSocket.on('connection', socket => {
        logger.userConnected(socket.request.connection, name, id);
        const observer = kernelInfo[name].subscribe(msg => socket.emit('msg', msg));
        socket.on('msg', msg => kernelInfo[name].next(msg));
        const disconnect = () => {
          if (!observer.isUnsubscribed) {
            observer.unsubscribe();
            logger.userDisconnected(socket.request.connection, name, id);
          }
        };
        socket.on('disconnect', disconnect);
        disconnectedSockets.then(disconnect);
      });
    }

    connectSocketZmq(kernelInfo.shellSocket, 'shell');
    connectSocketZmq(kernelInfo.stdinSocket, 'stdin');
    connectSocketZmq(kernelInfo.iopubSocket, 'iopub');
    connectSocketZmq(kernelInfo.controlSocket, 'control');

    res.send(JSON.stringify({id: id}));
  }).catch(err => {
    res.status(500).send(JSON.stringify({error: String(err)}));
  });
});

app.get('/shutdown/*', function(req, res) {
  res.header('Access-Control-Allow-Origin', '*');

  const id = req.url.split('/').slice(-1)[0];
  const kernelInfo = kernels[id];
  if (!kernelInfo) {
    res.status(500).send(JSON.stringify({error: 'kernel doesn\' exist'}));
    return;
  }

  const shutDownRequest = kernelInfo.createMessage('shutdown_request');
  shutDownRequest.content = {
    restart: false
  };

  const shutDownReply = kernelInfo.shell
    .filter(isChildMessage.bind(shutDownRequest))
    .filter(msg => msg.header.msg_type === 'shutdown_reply')
    .map(msg => msg.content)
    .subscribe(content => {
      if (!content.restart) {
        try {

          // Clean-up kernel resources
          kernelInfo.kernel.spawn.kill();
          kernelInfo.disconnectSockets();
          kernelInfo.shell.complete();
          kernelInfo.stdin.complete();
          kernelInfo.iopub.complete();
          kernelInfo.control.complete();
          fs.unlink(kernelInfo.kernel.connectionFile);

          // Clean-up socket.io namespaces
          delete io.nsps['/shell/' + id];
          delete io.nsps['/stdin/' + id];
          delete io.nsps['/iopub/' + id];
          delete io.nsps['/control/' + id];

          // Send success
          res.send(JSON.stringify({id: id}));
        } catch(error) {
          res.status(500).send(JSON.stringify({error: String(error)}));
        }
        delete kernels[id];
        logger.kernelStopped(id);
      }
    });
  kernelInfo.shell.next(shutDownRequest);
});

app.get('/list', function(req, res) {
  res.header('Access-Control-Allow-Origin', 'localhost');
  res.send(JSON.stringify(Object.keys(kernels)));
});

app.get('/specs', function (req, res) {
  res.header('Access-Control-Allow-Origin', 'localhost');
  kernelspecs.findAll().then((kernels) => {
    res.send(JSON.stringify(kernels))
  });
})

exports.listen = function listen(port) {
  http.listen(port, () => logger.startServer(port));
};
