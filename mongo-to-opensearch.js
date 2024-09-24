const { MongoClient } = require('mongodb');
const { Client } = require('@opensearch-project/opensearch');

// MongoDB connection details
const MONGO_URI = 'mongodb://localhost:27017';
const DATABASE_NAME = 'emp_mgt';
const COLLECTION_NAME = 'blogs';

// OpenSearch connection details
const OPENSEARCH_HOST = 'http://localhost:9200';
const INDEX_NAME = 'blogs';

// Initialize OpenSearch client
const openSearchClient = new Client({ node: OPENSEARCH_HOST });

// Function to insert data into OpenSearch
async function indexDataToOpenSearch(documents) {
    const bulkOps = [];

    // Prepare bulk operations for OpenSearch
    documents.forEach((doc) => {
        bulkOps.push({ index: { _index: INDEX_NAME } });   // Use MongoDB's _id as the document ID in OpenSearch
        bulkOps.push(doc);  // The actual document data
    });

    console.log('Bulk Operations:');
    console.log(JSON.stringify(bulkOps, null, 2));  // Log bulk operations to ensure correct data is being sent

    // Perform bulk indexing
    const bulkResponse = await openSearchClient.bulk({
      refresh: true,
      body: bulkOps,
    });

    // Check for errors in the bulk operation
    if (bulkResponse.errors) {
        bulkResponse.items.forEach(item => {
          if (item.index && item.index.error) {
            console.log('Error for document ID:', item.index._id);
            console.log('Error details:', JSON.stringify(item.index.error, null, 2));
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

      console.log(`Fetched ${documents.length} documents from MongoDB:::`);
      console.log('**', JSON.stringify(documents, null, 2)); // Log the documents to see if they are fetched correctly

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
