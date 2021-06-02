//@ts-check
const CosmosClient = require("@azure/cosmos").CosmosClient;
const CreateBulk = require("@azure/cosmos").BulkOperationType.Create;
const faker = require("faker");
const pLimit = require("p-limit");
const config = require("./config");
const url = require("url");

const endpoint = config.endpoint;
const key = config.key;

const databaseId = config.database.id;
const containerId = config.container.id;
const partitionKey = { kind: "Hash", paths: ["/Country"] };

const client = new CosmosClient({ endpoint, key });

const bulkSize = 100 // 100 is a maximum for bulk with same partition key.
const documentCount = 100000

/**
 * Create the database if it does not exist
 */
async function createDatabase() {
  const { database } = await client.databases.createIfNotExists({
    id: databaseId,
  });
  console.log(`Created database:\n${database.id}\n`);
}

/**
 * Read the database definition
 */
async function readDatabase() {
  const { resource: databaseDefinition } = await client
    .database(databaseId)
    .read();
  console.log(`Reading database:\n${databaseDefinition.id}\n`);
}

/**
 * Create the container if it does not exist
 */
async function createContainer() {
  const { container } = await client
    .database(databaseId)
    .containers.createIfNotExists(
      { id: containerId, partitionKey, maxThroughput: 10000 }
    );
  console.log(`Created container:\n${config.container.id}\n`);
}

/**
 * Read the container definition
 */
async function readContainer() {
  const { resource: containerDefinition } = await client
    .database(databaseId)
    .container(containerId)
    .read();
  console.log(`Reading container:\n${containerDefinition.id}\n`);
}

/**
 * Query the container using SQL
 */
async function queryContainer() {
  console.log(`Querying container:\n${config.container.id}`);

  // query to return all children in a family
  const querySpec = {
    query: "SELECT TOP 100 r.children FROM root r"
  };

  const { resources: results } = await client
    .database(databaseId)
    .container(containerId)
    .items.query(querySpec)
    .fetchAll();
  for (var queryResult of results) {
    let resultString = JSON.stringify(queryResult);
    console.log(`\tQuery returned ${resultString}\n`);
  }
}

/**
 * Exit the app with a prompt
 * @param {string} message - The message to display
 */
function exit(message) {
  console.log(message);
  console.log("Press any key to exit");
  process.stdin.setRawMode(true);
  process.stdin.resume();
  process.stdin.on("data", process.exit.bind(process, 0));
}

createDatabase()
  .then(() => readDatabase())
  .then(() => createContainer())
  .then(() => readContainer())
  .then(() => generateDataAndBulkUpload())
  .then(() => queryContainer())
  .then(() => {
    console.timeEnd("cosmos")
    exit(`Completed successfully`);
  })
  .catch((error) => {
    console.log(error);
    exit(`Completed with error ${JSON.stringify(error)}`);
  });

function generateData() {
  var randomPerson = {
    id: faker.datatype.uuid(),
    Country: faker.address.county(),
    //key: faker.address.countryCode(),
    lastName: faker.name.lastName(),
    parents: [
      {
        firstName: faker.name.firstName(0),
      },
      {
        firstName: faker.name.firstName(1),
      },
    ],
    children: [
      {
        firstName: faker.name.firstName(),
        gender: faker.name.gender(),
        jobTitle: faker.name.jobTitle(),
        pets: [
          {
            givenName: faker.name.firstName(),
          },
        ],
      },
    ],
    address: {
      state: faker.address.state(),
      county: faker.address.county(),
      city: faker.address.city(),
    },
  };

  //console.log(JSON.stringify(randomPerson, null, 2))

  return randomPerson;
}

function generateDataAndBulkUpload() {
  var personList = [];
  console.time("cosmos");
  var bulkOperations = [];

  for (var i = 0; i < documentCount; i++) {
    personList[i] = generateData();
  }
  for (var i = 0; i < personList.length / bulkSize; i++) {
    var personSlice = personList.slice(i * bulkSize, i * bulkSize + bulkSize);
    var operations = personSlice.map(bulkBody);
    bulkOperations[i] = operations;
  }

  console.timeLog("cosmos");
  return Promise.all(limitedBulkUpload(bulkOperations));
}


function limitedBulkUpload(bulkoperations) {

const limit = pLimit(1);
  // Missing Error Handling
  // Missing Retry Pattern
return bulkoperations.map((data) => limit(() => bulkUpload(data)));

}

function bulkBody(data) {

    return {
      operationType: CreateBulk,
      resourceBody: data,
    };

}

function bulkUpload(data) {

  return client
      .database(databaseId)
      .container(containerId)
      .items.bulk(data)
      .then((result) => {
        console.timeLog("cosmos")
        var filteredlist = result.filter((elem) => {
          return elem.statusCode != 201;
        });
        if(filteredlist.length > 0) {
          throw filteredlist
        }
      })
}