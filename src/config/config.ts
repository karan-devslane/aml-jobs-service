// import dotenv from 'dotenv';
import { get, isEqual } from 'lodash';
import { IConfiguration } from './interfaces';

const envVars = process.env;

const appConfiguration: IConfiguration = {
  log: {
    day: get(envVars, 'LOG_FILE_DAY', '14d'),
    isEnable: isEqual(get(envVars, 'LOG_FILE_ENABLE', 'true'), 'true'),
    name: get(envVars, 'LOG_FILE_NAME', 'AML-log'),
    size: get(envVars, 'LOG_FILE_SIZE', '20m'),
    zippedArchive: isEqual(get(envVars, 'LOG_FILE_ZIP_ARCHIVE', 'false'), 'true'),
  },
  envPort: get(envVars, 'AML_SERVICE_APPLICATION_PORT', '4000'),
  applicationEnv: get(envVars, 'AML_SERVICE_APPLICATION_ENV', 'development'),
  appVersion: get(envVars, 'AML_SERVICE_APP_VERSION', '1.1'),
  DB: {
    host: get(envVars, 'AML_SERVICE_DB_HOST', 'localhost'),
    port: Number(get(envVars, 'AML_SERVICE_DB_PORT')),
    password: get(envVars, 'AML_SERVICE_DB_PASS', 'postgres'),
    name: get(envVars, 'AML_SERVICE_DB_NAME', 'bulk_upload_service'),
    user: get(envVars, 'AML_SERVICE_DB_USER', 'postgres'),
  },
  bucketName: get(envVars, 'BUCKET_NAME', ''),
  accessKeyId: get(envVars, 'ACCESS_KEY_ID', ''),
  secretAccessKey: get(envVars, 'SECRET_KEY', ''),
  region: get(envVars, 'REGION', ''),
  fibCsvFileName: get(envVars, 'FIB_CSV_FILE_NAME') as unknown as string[],
  mcqCsvFileName: get(envVars, 'MCQ_CSV_FILE_NAME') as unknown as string[],
  gridCsvFileName: get(envVars, 'GRID_CSV_FILE_NAME') as unknown as string[],
};

export default appConfiguration;
