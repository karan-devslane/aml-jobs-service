import logger from '../utils/logger';
import * as _ from 'lodash';
import { getAWSFolderData, uploadCsvFile } from '../services/awsService';
import AdmZip from 'adm-zip';
import { Parser } from '@json2csv/plainjs';
import { getBoards, getClasses, getRepository, getSkills, getSubSkills, getTenants } from '../services/service';

let FILENAME: string;
let Process_id: string;

export const fetchAndExtractZipEntries = async (folderName: string, process_id: string, fileName: string) => {
  Process_id = process_id;
  FILENAME = fileName;
  try {
    let bulkUploadFolder;
    if (folderName === 'upload') {
      bulkUploadFolder = await getAWSFolderData(`upload/${process_id}/${fileName}`);
    } else {
      bulkUploadFolder = await getAWSFolderData(`template/${fileName}`);
    }
    const buffer = (await streamToBuffer(bulkUploadFolder)) as Buffer;
    const zip = new AdmZip(buffer);
    logger.info('Cloud Process:: converted stream to zip entries');
    return {
      error: null,
      result: {
        isValid: true,
        data: zip.getEntries(),
      },
    };
  } catch (error) {
    const code = _.get(error, 'code', 'UPLOAD_QUESTION_CRON');
    const errorMsg = error instanceof Error ? error.message : 'Error in the validation process,please re-upload the zip file for the new process';
    logger.error({ errorMsg, code });
    return {
      error: null,
      result: {
        isValid: false,
        data: [],
      },
    };
  }
};

export const streamToBuffer = (stream: any) => {
  return new Promise((resolve, reject) => {
    const chunks: any = [];
    stream.on('data', (chunk: any) => chunks.push(chunk));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
  });
};

export const getCSVTemplateHeader = async (entryName: string) => {
  const templateZipEntries = await fetchAndExtractZipEntries('template', Process_id, FILENAME);
  const templateFileContent = templateZipEntries.result.data
    .find((t) => t.entryName === entryName)
    ?.getData()
    .toString('utf8');
  if (!templateFileContent) {
    logger.error(`Template:: The file '${entryName}' does not match the expected CSV format.`);
    return {
      error: { errStatus: 'invalid_template', errMsg: `Template for '${entryName}' not found.`, status: 'failed' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  const [templateHeader] = templateFileContent.split('\n').map((row) => row.split(','));
  logger.info('Template:: template header extracted.');
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: templateHeader,
    },
  };
};

export const getCSVHeaderAndRow = (csvEntries: any) => {
  const [header, ...rows] = csvEntries
    .getData()
    .toString('utf8')
    .split('\n')
    .map((row: string) => row.split(','))
    .filter((row: string[]) => row.some((cell) => cell.trim() !== ''));
  logger.info('Row/Header:: header and rows are extracted');
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: { header, rows },
    },
  };
};

export const validHeader = (entryName: string, header: any, templateHeader: any) => {
  if (header.length !== templateHeader.length) {
    logger.error(`Header Validate:: CSV file contains more/less fields compared to the template.`);
    return {
      error: { errStatus: null, errMsg: null },
      result: {
        isValid: false,
        data: null,
      },
    };
  }

  const validHeader = templateHeader.every((col: any, i: number) => col === header[i]);
  if (!validHeader) {
    logger.error(`Header validate:: The file '${entryName}' does not match the expected CSV format.`);
    return {
      error: { errStatus: 'Header validate', errMsg: `The file '${entryName}' does not match the expected CSV format.` },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  logger.info(`Header validate:: ${entryName} contain valid header`);
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: null,
    },
  };
};

export const processRow = (rows: string[][], header: string[]) => {
  return rows.map((row) =>
    row.reduce(
      (acc, cell, index) => {
        const headerName = header[index].replace(/\r/g, '');
        const cellValue = cell.includes('#') ? cell.split('#').map((v: string) => v.trim()) : cell.replace(/\r/g, '');
        if (headerName.startsWith('mcq') || headerName.startsWith('fib') || headerName.startsWith('grid') || headerName.includes('n1') || headerName.includes('n2')) {
          acc.body = acc.body || {};
          acc.body[headerName] = cellValue;
        } else if (headerName.includes('L2_skill') || headerName.includes('L3_skill') || headerName.includes('sub_skill')) {
          acc[headerName] = typeof cellValue === 'string' ? [cellValue] : cellValue;
        } else if (headerName.includes('media')) {
          acc.media_files = acc.media_files || [];
          if (cellValue) acc.media_files.push(cellValue);
        } else if (headerName.includes('QSID')) {
          acc['question_set_id'] = cellValue;
        } else if (headerName.includes('QID')) {
          acc['question_id'] = cellValue;
        } else if (headerName.includes('sequence') || headerName.includes('benchmark_time')) {
          acc[headerName] = Number(cellValue);
        } else if (headerName.includes('sub_skill_x+x')) {
          acc['sub_skill_xx'] = cellValue;
        } else if (headerName.includes('sub_skill_x+0')) {
          acc['sub_skill_x0'] = cellValue;
        } else if (headerName.includes('is_atomic')) {
          acc['is_atomic'] = cellValue.toLocaleString().toLowerCase() === 'true';
        } else if (headerName.includes('instruction_media')) {
          acc['is_atomic'] = cellValue;
        } else if (headerName.includes('instruction_text')) {
          acc['is_atomic'] = cellValue;
        } else {
          acc[headerName] = cellValue;
        }
        acc.process_id = Process_id;
        return acc;
      },
      {} as Record<string, any>,
    ),
  );
};
export const convertToCSV = async (jsonData: any, fileName: string) => {
  const json2csvParser = new Parser();
  const csv = json2csvParser.parse(jsonData);
  const uploadMediaFile = await uploadCsvFile(csv, `upload/${Process_id}/${fileName}.csv`);
  logger.info(`CSV:: csv file created from staging data for ${fileName}`);
  return uploadMediaFile;
};

export const preloadData = async () => {
  const [boards, classes, skills, subSkills, tenants, repositories] = await Promise.all([getBoards(), getClasses(), getSkills(), getSubSkills(), getTenants(), getRepository()]);
  logger.info('Preloaded:: preloading metadata from table.');
  return {
    boards,
    classes,
    skills,
    tenants,
    subSkills,
    repositories,
  };
};
