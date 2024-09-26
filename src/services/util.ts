import logger from '../utils/logger';
import { getAWSFolderData, uploadCsvFile } from '../services/awsService';
import AdmZip from 'adm-zip';
import { Parser } from '@json2csv/plainjs';
import { getBoards, getClasses, getRepository, getSkills, getSubSkills, getTenants } from '../services/service';
import { appConfiguration } from '../config';
const { bulkUploadFolder, templateFileName, templateFolder } = appConfiguration;

let processId: string;

export const fetchAndExtractZipEntries = async (key: string, process_id: string, fileName?: string) => {
  processId = process_id;
  try {
    let bulkUploadStream;
    if (key === 'upload') {
      bulkUploadStream = await getAWSFolderData(`${bulkUploadFolder}/${process_id}/${fileName}`);
    } else {
      bulkUploadStream = await getAWSFolderData(`${templateFolder}/${templateFileName}`);
    }
    const bulkUploadBuffer = (await streamToBuffer(bulkUploadStream)) as Buffer;
    const bulkUploadZip = new AdmZip(bulkUploadBuffer);
    logger.info('Cloud Process:: converted stream to zip entries');
    return {
      error: null,
      result: {
        isValid: true,
        data: bulkUploadZip.getEntries(),
      },
    };
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error in the validation process,please re-upload the zip file for the new process';
    logger.error(errorMsg);
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
  const templateZipEntries = await fetchAndExtractZipEntries('template', processId);
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
  const cleanHeader = templateHeader.map((cell: string) => cell.replace(/\r/g, '').trim());
  logger.info('Template:: template header extracted.');
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: cleanHeader,
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
  const cleanHeader = header.map((cell: string) => cell.replace(/\r/g, '').trim());
  const cleanRows = rows.map((row: any) => row.map((cell: string) => cell.replace(/\r/g, '').trim()));
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: { header: cleanHeader, rows: cleanRows },
    },
  };
};

export const validateHeader = (entryName: string, header: any, templateHeader: any) => {
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
          acc['instruction_media'] = cellValue;
        } else if (headerName.includes('instruction_text')) {
          acc['instruction_media'] = cellValue;
        } else {
          acc[headerName] = cellValue;
        }
        acc.process_id = processId;
        return acc;
      },
      {} as Record<string, any>,
    ),
  );
};
export const convertToCSV = async (jsonData: any, fileName: string) => {
  const json2csvParser = new Parser();
  const csv = json2csvParser.parse(jsonData);
  const uploadMediaFile = await uploadCsvFile(csv, `upload/${processId}/${fileName}.csv`);
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
