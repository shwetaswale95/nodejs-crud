const { MongoClient } = require('mongodb');
const { Client } = require('@opensearch-project/opensearch');

// MongoDB connection details
const MONGO_URI = 'mongodb://localhost:27017';
const DATABASE_NAME = 'emp_mgt';
const COLLECTION_NAME = 'blogs';

// OpenSearch connection details
const OPENSEARCH_HOST = 'http://localhost:9200';
const INDEX_NAME = 'blogs_v2';

// Initialize OpenSearch client
const openSearchClient = new Client({ node: OPENSEARCH_HOST });

// Function to insert data into OpenSearch
async function indexDataToOpenSearch(documents) {
  const bulkOps = [];

  documents.forEach((doc) => {
    bulkOps.push({ index: { _index: INDEX_NAME, _id: doc._id } }); // Include _id here
    bulkOps.push({
      title: doc.title,
      content: doc.content,
      address: doc.address,
      first_name: doc.first_name,
    });
  });

  console.log('Bulk Operations:', JSON.stringify(bulkOps, null, 2));

  const bulkResponse = await openSearchClient.bulk({
    refresh: true,
    body: bulkOps,
  });

  if (bulkResponse.errors) {
    bulkResponse.items.forEach(item => {
      if (item.index && item.index.error) {
        console.error('Error for document ID:', item.index._id, 'Error details:', JSON.stringify(item.index.error, null, 2));
      }
    });
  } else {
    console.log('Successfully indexed documents to OpenSearch');
  }
}

// Main function to fetch data from MongoDB and index into OpenSearch
async function reindexMongoToOpenSearch() {
  const client = new MongoClient(MONGO_URI);

  try {
    // Connect to MongoDB
    await client.connect();
    const db = client.db(DATABASE_NAME);
    const collection = db.collection(COLLECTION_NAME);

    // Fetch all documents from the MongoDB collection
    const documents = await collection.find({}).toArray();

    console.log(`Fetched ${documents.length} documents from MongoDB:`, JSON.stringify(documents, null, 2));

    // Index documents into OpenSearch
    await indexDataToOpenSearch(documents);
  } catch (err) {
    console.error('Error while reindexing:', err);
  } finally {
    // Close the MongoDB connection
    await client.close();
  }
}

// Start the reindexing process
reindexMongoToOpenSearch();
