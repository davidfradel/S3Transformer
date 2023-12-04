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
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.processCsvFile = void 0;
var client_s3_1 = require("@aws-sdk/client-s3");
var fs_1 = require("fs");
var csv_parser_1 = require("csv-parser");
var util_1 = require("util");
var stream_1 = require("stream");
var fast_csv_1 = require("fast-csv");
// Environnement variables configuration
var dotenv_1 = require("dotenv");
var process_1 = require("process");
dotenv_1.default.config();
if (!process_1.env.AWS_ACCESS_KEY_ID || !process_1.env.AWS_SECRET_ACCESS_KEY) {
    throw new Error('AWS credentials are missing');
}
// S3 Configuration
var s3Client = new client_s3_1.S3Client({
    credentials: {
        accessKeyId: process_1.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process_1.env.AWS_SECRET_ACCESS_KEY,
    }
});
var pipeline = (0, util_1.promisify)(stream_1.default.pipeline);
function downloadFileFromS3(bucket, key, localPath) {
    return __awaiter(this, void 0, void 0, function () {
        var getObjectParams, data, readableStream;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    getObjectParams = {
                        Bucket: bucket,
                        Key: key
                    };
                    return [4 /*yield*/, s3Client.send(new client_s3_1.GetObjectCommand(getObjectParams))];
                case 1:
                    data = _a.sent();
                    if (!data.Body) return [3 /*break*/, 3];
                    readableStream = data.Body;
                    return [4 /*yield*/, pipeline(readableStream, fs_1.default.createWriteStream(localPath))];
                case 2:
                    _a.sent();
                    _a.label = 3;
                case 3: return [2 /*return*/];
            }
        });
    });
}
function listAllFiles(bucketName) {
    var _a;
    return __awaiter(this, void 0, void 0, function () {
        var listObjectsParams, data;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    listObjectsParams = {
                        Bucket: bucketName
                    };
                    return [4 /*yield*/, s3Client.send(new client_s3_1.ListObjectsV2Command(listObjectsParams))];
                case 1:
                    data = _b.sent();
                    return [2 /*return*/, ((_a = data.Contents) === null || _a === void 0 ? void 0 : _a.map(function (file) { return file.Key; })) || []];
            }
        });
    });
}
function processCsvFile(filePath, segmentMappings) {
    return __awaiter(this, void 0, void 0, function () {
        var results;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    results = [];
                    return [4 /*yield*/, pipeline(fs_1.default.createReadStream(filePath), (0, csv_parser_1.default)({
                            separator: ';', // Specify the CSV separator
                            headers: ['url', 'segmentData'], // Specify the CSV headers
                        })
                            .on('data', function (row) {
                            // The first column is the URL, the second column is the segment data
                            var url = row.url;
                            var rawSegmentData = row.segmentData;
                            if (!rawSegmentData)
                                return; // Ignorez les lignes sans données de segment
                            var segments = rawSegmentData.split(',')
                                .map(function (s) { return s.split('='); })
                                .filter(function (_a) {
                                var _id = _a[0], score = _a[1];
                                return parseInt(score) >= 500;
                            })
                                .map(function (_a) {
                                var id = _a[0], _score = _a[1];
                                return segmentMappings[parseInt(id)];
                            })
                                .filter(function (name) { return name; }); // Filtrer les noms non trouvés dans segmentMappings
                            if (segments.length > 0) {
                                results.push({ url: url, segments: segments });
                            }
                        }))];
                case 1:
                    _a.sent();
                    return [2 /*return*/, results];
            }
        });
    });
}
exports.processCsvFile = processCsvFile;
function createCsvFile(data, filePath) {
    return new Promise(function (resolve, reject) {
        var csvStream = (0, fast_csv_1.format)({ headers: true });
        var writableStream = fs_1.default.createWriteStream(filePath);
        writableStream.on('finish', resolve);
        writableStream.on('error', reject);
        csvStream.pipe(writableStream);
        data.forEach(function (row) {
            // Convert each row to the expected format
            var csvRow = { url: row.url };
            row.segments.forEach(function (segment, index) {
                csvRow["segment".concat(index + 1)] = segment;
            });
            csvStream.write(csvRow);
        });
        csvStream.end();
    });
}
function uploadFileToS3(bucket, key, localPath) {
    return __awaiter(this, void 0, void 0, function () {
        var fileContent, putObjectParams;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    fileContent = fs_1.default.readFileSync(localPath);
                    putObjectParams = {
                        Bucket: bucket,
                        Key: key,
                        Body: fileContent
                    };
                    return [4 /*yield*/, s3Client.send(new client_s3_1.PutObjectCommand(putObjectParams))];
                case 1:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function run() {
    return __awaiter(this, void 0, void 0, function () {
        var bucketName, allFiles, segmentsLocalPath, segmentsArray, segmentMappings, csvFiles, _i, csvFiles_1, file, localCsvPath, processedData, newCsvFilePath, newCsvKey, error_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    bucketName = 'technicaltest';
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 11, , 12]);
                    return [4 /*yield*/, listAllFiles(bucketName)];
                case 2:
                    allFiles = _a.sent();
                    segmentsLocalPath = './files/segments.json';
                    return [4 /*yield*/, downloadFileFromS3(bucketName, 'segments.json', segmentsLocalPath)];
                case 3:
                    _a.sent();
                    segmentsArray = JSON.parse(fs_1.default.readFileSync(segmentsLocalPath, 'utf8'));
                    segmentMappings = segmentsArray.reduce(function (acc, segment) {
                        acc[segment.id] = segment.name;
                        return acc;
                    }, {});
                    csvFiles = allFiles.filter(function (file) { return !file.includes('/') && file.endsWith('.csv'); });
                    _i = 0, csvFiles_1 = csvFiles;
                    _a.label = 4;
                case 4:
                    if (!(_i < csvFiles_1.length)) return [3 /*break*/, 10];
                    file = csvFiles_1[_i];
                    console.log("csvFILES", csvFiles);
                    localCsvPath = "./files/".concat(file);
                    return [4 /*yield*/, downloadFileFromS3(bucketName, file, localCsvPath)];
                case 5:
                    _a.sent();
                    return [4 /*yield*/, processCsvFile(localCsvPath, segmentMappings)];
                case 6:
                    processedData = _a.sent();
                    console.log("Processed data for ".concat(file));
                    newCsvFilePath = './files/processed_data.csv';
                    // Créer le fichier CSV
                    return [4 /*yield*/, createCsvFile(processedData, newCsvFilePath)];
                case 7:
                    // Créer le fichier CSV
                    _a.sent();
                    newCsvKey = "submissions/david_fradel-2023/".concat(file);
                    return [4 /*yield*/, uploadFileToS3(bucketName, newCsvKey, newCsvFilePath)];
                case 8:
                    _a.sent();
                    console.log('CSV file processed and uploaded successfully.');
                    _a.label = 9;
                case 9:
                    _i++;
                    return [3 /*break*/, 4];
                case 10:
                    process.exit();
                    return [3 /*break*/, 12];
                case 11:
                    error_1 = _a.sent();
                    console.error('Error:', error_1);
                    return [3 /*break*/, 12];
                case 12: return [2 /*return*/];
            }
        });
    });
}
run();
