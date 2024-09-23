// import dotenv from 'dotenv';
import { get, isEqual } from 'lodash';
import { IConfiguration } from './interfaces';

const envVars = process.env;

const appConfiguration: IConfiguration = {
  log: {
    day: get(envVars, 'LOG_FILE_DAY', '14d'),
    isEnable: isEqual(get(envVars, 'LOG_FILE_ENABLE', 'true'), 'true'),
    name: get(envVars, 'LOG_FILE_NAME', 'AML-job-log'),
    size: get(envVars, 'LOG_FILE_SIZE', '20m'),
    zippedArchive: isEqual(get(envVars, 'LOG_FILE_ZIP_ARCHIVE', 'false'), 'true'),
  },
  envPort: get(envVars, 'AML_SERVICE_APPLICATION_PORT', 4000) as number,
  applicationEnv: get(envVars, 'AML_SERVICE_APPLICATION_ENV', 'development'),
  appVersion: get(envVars, 'AML_SERVICE_APP_VERSION', '1.0'),
  DB: {
    host: get(envVars, 'AML_SERVICE_DB_HOST', 'localhost'),
    port: Number(get(envVars, 'AML_SERVICE_DB_PORT')),
    password: get(envVars, 'AML_SERVICE_DB_PASS', 'postgres'),
    name: get(envVars, 'AML_SERVICE_DB_NAME', 'postgres'),
    user: get(envVars, 'AML_SERVICE_DB_USER', 'postgres'),
  },
  bucketName: get(envVars, 'BUCKET_NAME', ''),
  csvFileName: get(envVars, 'CSV_FILE_NAME', '').split(','),
  presignedUrlExpiry: get(envVars, 'PRESIGNED_URL_EXPIRY_TIME', 600) as number,
  processInterval: get(envVars, 'PROCESS_INTERVAL', 300000) as number,
  reCheckProcessInterval: get(envVars, 'RE_CHECK_PROCESS_INTERVAL', 4) as number, //hours
  fileUploadInterval: get(envVars, 'FILE_UPLOAD_INTERVAL', 4) as number, //hours
  grid1AddFields: get(envVars, 'GRID-1_ADD_FIELDS', '').split(','),
  grid1SubFields: get(envVars, 'GRID-1_SUB_FIELDS', '').split(','),
  grid1MultipleFields: get(envVars, 'GRID-1_MULTIPLE_FIELDS', '').split(','),
  grid1DivFields: get(envVars, 'GRID-1_DIVISION_FIELDS', '').split(','),
  grid2Fields: get(envVars, 'GRID-2_FIELDS', '').split(','),
  fibFields: get(envVars, 'FIB_FIELDS', '').split(','),
  mcqFields: get(envVars, 'MCQ_FIELDS', '').split(','),
};

export default appConfiguration;
