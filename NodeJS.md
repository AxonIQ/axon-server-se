# Calling MessagingPlatform from NodeJS

When running NodeJS services that integrate with the AxonFramework applications, you can use 
the Proto Javascript bindings to send commands and queries.

## Setup GRPC for NodeJS

Install the gRPC NPM package
```
npm install grpc
```

Generate Javascript files based on proto files.

```
export PATH=/c/Users/marc/AppData/Roaming/npm/grpc_tools_node_protoc_plugin:$PATH

grpc_tools_node_protoc --js_out=import_style=commonjs,binary:out --grpc_out=out messaging_api.proto messaging_messages.proto
```

Output is stored in the out folder.

Create package.json file in the out folder with dependencies for NodeJS

```json
{
  "name": axonhub,
  "version": "0.1.0",
  "dependencies": {
    "async": "^1.5.2",
    "google-protobuf": "^3.0.0",
    "grpc": "^1.0.0",
    "lodash": "^4.6.1",
    "minimist": "^1.2.0"
  }
}
```

And run ```npm install``` to download and install the dependencies.

# Running a command

```javascript
var messaging_messages_pb = require('./messaging_messages_pb');
var services = require('./messaging_api_grpc_pb');
var uuid = require('node-uuid');

var grpc = require('grpc');


function runCommand(args) {
  var messagingServerName = new services.CommandServiceClient('localhost:8000',
                                          grpc.credentials.createInsecure());
  var request = new messaging_messages_pb.Command();
  var serializedObject = new messaging_messages_pb.SerializedObject();
  request.setMessageIdentifier(uuid.v1());
  request.setName('io.axoniq.sample.EchoCommand');
  request.setPayload(serializedObject);
  serializedObject.setType('io.axoniq.sample.EchoCommand');
  buff = new Buffer('<io.axoniq.sample.EchoCommand><id>' + request.getMessageIdentifier() + '</id><text>Text</text></io.axoniq.sample.EchoCommand>');
  serializedObject.setData(buff.toString('base64'));
  
  
  messagingServerName.dispatch(request, function(err, response) {
    if ( ! err) {
      console.log('ID:', response.getMessageIdentifier());
      if( response.getSuccess() ) {
          buff = new Buffer(response.getPayload().getData(), 'base64');
          console.log('Payload:', buff.toString('ascii'));
      } else {
          console.log('Message:', response.getMessage());
      }
    } else {
        console.log('Error:', err);
    }
  });
}

runCommand(process.argv.slice(2));
```

# Running a query
```javascript
var messaging_messages_pb = require('./messaging_messages_pb');
var services = require('./messaging_api_grpc_pb');
var uuid = require('node-uuid');

var grpc = require('grpc');

function runQuery(args) {
  var messagingServerName = new services.QueryServiceClient('localhost:8000',
                                          grpc.credentials.createInsecure());
  var request = new messaging_messages_pb.QueryRequest();
  var serializedObject = new messaging_messages_pb.SerializedObject();
  request.setMessageIdentifier(uuid.v1());
  request.setQuery('java.lang.String');
  request.setResultName('java.lang.String');
  request.setPayload(serializedObject);
  if( args.length > 0) request.setNumberOfResults(parseInt(args[0]));
  serializedObject.setType('string');
  buff = new Buffer('<string>' + request.getMessageIdentifier() + '</string>');
  serializedObject.setData(buff.toString('base64'));
  
  var timeout_in_seconds = 15
  var timeout = new Date().setSeconds(new Date().getSeconds() + timeout_in_seconds)

  var s = messagingServerName.query(request, {"deadline": timeout});
  s.on('data', function(d) {
      buff = new Buffer(d.getPayload().getData(), 'base64');
      console.log(buff.toString('ascii'));
  })
  s.on('end', function() {
      console.log('End');
  })
  s.on('status', function(d) {
      console.log('Status: ' + JSON.stringify(d));
  })
}

runQuery(process.argv.slice(2));
```

 