import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import type { ProtoGrpcType as BlobType } from './proto/blob';
import type { StoreResponse } from './proto/buffdb/blob/StoreResponse';

const proto = grpc.loadPackageDefinition(protoLoader.loadSync('../../proto/blob.proto')) as unknown as BlobType;

const blob_client = new proto.buffdb.blob.Blob('[::1]:50051', grpc.credentials.createInsecure());
const get = blob_client.Get();
const store = blob_client.Store();

// Be sure to set up the listeners before writing data!

get.on('data', data => console.log('received data from GET: ', data));
get.on('end', () => console.log('stream GET ended'));

const blob_ids: number[] = [];
store.on('data', (raw_id: StoreResponse) => {
    const id = (raw_id.id as protoLoader.Long).toNumber();
    console.log('received data from STORE: ', id);
    blob_ids.push(id);
});
store.on('end', () => console.log('stream STORE ended'));

store.write({ bytes: Buffer.from('abcdef'), metadata: "{ offset: 6 }" });
store.write({ bytes: Buffer.from('ghijkl') });

store.end();

// Give the store time to finish its operations before asking for data back.
// We could also do this in the callback of other events to be certain that it's been inserted.
setTimeout(() => {
    for (const id of blob_ids) {
        console.log(`requesting ${id}`);
        get.write({ id });
    }
    get.end();
}, 100);
