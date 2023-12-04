"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.processCsvFile = void 0;
const client_s3_1 = require("@aws-sdk/client-s3");
const fs_1 = __importDefault(require("fs"));
const csv_parser_1 = __importDefault(require("csv-parser"));
const util_1 = require("util");
const stream_1 = __importDefault(require("stream"));
const fast_csv_1 = require("fast-csv");
// Environnement variables configuration
const dotenv_1 = __importDefault(require("dotenv"));
const process_1 = require("process");
dotenv_1.default.config();
if (!process_1.env.AWS_ACCESS_KEY_ID || !process_1.env.AWS_SECRET_ACCESS_KEY || !process_1.env.AWS_DEFAULT_REGION) {
    throw new Error('AWS credentials are missing');
}
if (!process_1.env.S3_BUCKET_NAME) {
    throw new Error('Bucket name is missing');
}
// S3 Configuration
const s3Client = new client_s3_1.S3Client({
    region: process_1.env.AWS_DEFAULT_REGION,
    credentials: {
        accessKeyId: process_1.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process_1.env.AWS_SECRET_ACCESS_KEY,
    }
});
const pipeline = (0, util_1.promisify)(stream_1.default.pipeline);
function downloadFileFromS3(bucket, key, localPath) {
    return __awaiter(this, void 0, void 0, function* () {
        const getObjectParams = {
            Bucket: bucket,
            Key: key
        };
        const data = yield s3Client.send(new client_s3_1.GetObjectCommand(getObjectParams));
        if (data.Body) {
            const readableStream = data.Body; // Conversion explicite
            yield pipeline(readableStream, fs_1.default.createWriteStream(localPath));
        }
    });
}
function listAllFiles(bucketName) {
    var _a;
    return __awaiter(this, void 0, void 0, function* () {
        const listObjectsParams = {
            Bucket: bucketName
        };
        const data = yield s3Client.send(new client_s3_1.ListObjectsV2Command(listObjectsParams));
        return ((_a = data.Contents) === null || _a === void 0 ? void 0 : _a.map(file => file.Key)) || [];
    });
}
function processCsvFile(filePath, segmentMappings) {
    return __awaiter(this, void 0, void 0, function* () {
        const results = [];
        yield pipeline(fs_1.default.createReadStream(filePath), (0, csv_parser_1.default)({
            separator: ';', // Specify the CSV separator
            headers: ['url', 'segmentData'], // Specify the CSV headers
        })
            .on('data', (row) => {
            // The first column is the URL, the second column is the segment data
            const url = row.url;
            const rawSegmentData = row.segmentData;
            if (!rawSegmentData)
                return; // Ignorez les lignes sans données de segment
            const segments = rawSegmentData.split(',')
                .map(s => s.split('='))
                .filter(([_id, score]) => parseInt(score) >= 500)
                .map(([id, _score]) => segmentMappings[parseInt(id)])
                .filter(name => name); // Filtrer les noms non trouvés dans segmentMappings
            if (segments.length > 0) {
                results.push({ url, segments });
            }
        }));
        return results;
    });
}
exports.processCsvFile = processCsvFile;
function createCsvFile(data, filePath) {
    return new Promise((resolve, reject) => {
        const csvStream = (0, fast_csv_1.format)({ headers: true });
        const writableStream = fs_1.default.createWriteStream(filePath);
        writableStream.on('finish', resolve);
        writableStream.on('error', reject);
        csvStream.pipe(writableStream);
        data.forEach(row => {
            // Convert each row to the expected format
            const csvRow = { url: row.url };
            row.segments.forEach((segment, index) => {
                csvRow[`segment${index + 1}`] = segment;
            });
            csvStream.write(csvRow);
        });
        csvStream.end();
    });
}
function uploadFileToS3(bucket, key, localPath) {
    return __awaiter(this, void 0, void 0, function* () {
        // Read content from the file and upload it to the S3 bucket
        const fileContent = fs_1.default.readFileSync(localPath);
        const putObjectParams = {
            Bucket: bucket,
            Key: key,
            Body: fileContent
        };
        yield s3Client.send(new client_s3_1.PutObjectCommand(putObjectParams));
    });
}
function run() {
    return __awaiter(this, void 0, void 0, function* () {
        const bucketName = process_1.env.S3_BUCKET_NAME;
        try {
            // List all files in the bucket
            const allFiles = yield listAllFiles(bucketName);
            // Download the segments.json file and parse it
            const segmentsLocalPath = './files/segments.json';
            yield downloadFileFromS3(bucketName, 'segments.json', segmentsLocalPath);
            const segmentsArray = JSON.parse(fs_1.default.readFileSync(segmentsLocalPath, 'utf8'));
            const segmentMappings = segmentsArray.reduce((acc, segment) => {
                acc[segment.id] = segment.name;
                return acc;
            }, {});
            // Filter the CSV files
            const csvFiles = allFiles.filter(file => !file.includes('/') && file.endsWith('.csv'));
            for (const file of csvFiles) {
                const localCsvPath = `./files/${file}`;
                yield downloadFileFromS3(bucketName, file, localCsvPath);
                const processedData = yield processCsvFile(localCsvPath, segmentMappings);
                console.log(`Processed data for ${file}`);
                // Chemin où le nouveau fichier CSV sera enregistré
                const newCsvFilePath = './files/processed_data.csv';
                // Create the new CSV file 
                yield createCsvFile(processedData, newCsvFilePath);
                // Download the new CSV file to S3
                const newCsvKey = `submissions/david_fradel-2023/${file}`;
                yield uploadFileToS3(bucketName, newCsvKey, newCsvFilePath);
                console.log('CSV file processed and uploaded successfully.');
            }
            console.log('All files processed successfully ! 👍 👍 👍');
            process.exit();
        }
        catch (error) {
            console.error('Error:', error);
        }
    });
}
run();
