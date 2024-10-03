import logger from '../utils/logger';
import { getAWSFolderData, uploadCsvFile } from '../services/awsService';
import AdmZip from 'adm-zip';
import _ from 'lodash';
import { Parser } from '@json2csv/plainjs';
import { getBoards, getClasses, getRepository, getSkills, getSubSkills, getTenants } from '../services/service';
import { appConfiguration } from '../config';
import { Board, Class, Skill, SubSkill, UniqueValues, Mismatches } from '../types/util';

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
  try {
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
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error while accessing the row and header';
    return {
      error: { errStatus: 'Unexpected error', errMsg: errorMsg },
      result: {
        isValid: true,
        data: { header: '', rows: '' },
      },
    };
  }
};

export const validateHeader = (entryName: string, header: any, templateHeader: any) => {
  if (header.length !== templateHeader.length) {
    logger.error(`Header Validate:: CSV file contains more/less fields compared to the template.`);
    return {
      error: { errStatus: 'Header validate', errMsg: `The file '${entryName}' does not matched with header length.` },
      result: {
        isValid: false,
        data: null,
      },
    };
  }

  const mismatchedColumns: string[] = [];
  const validHeader = templateHeader.every((col: string, i: number) => {
    if (col !== header[i]) {
      mismatchedColumns.push(`Expected: '${col}', Found: '${header[i]}'`);
      return false;
    }
    return true;
  });
  if (!validHeader) {
    logger.error(`Header validate:: The file '${entryName}' does not match the expected CSV format. Mismatched columns: ${mismatchedColumns.join(', ')}`);
    return {
      error: { errStatus: 'Header validate', errMsg: `The file '${entryName}' does not match the exact column names. Mismatched columns: ${mismatchedColumns.join(', ')}` },
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
        if (headerName.includes('grid1_show_carry')) {
          acc[headerName] = cellValue === undefined ? 'no' : cellValue;
        }
        if (headerName.includes('grid1_show_regroup')) {
          acc[headerName] = cellValue === undefined ? 'no' : cellValue;
        }
        if (headerName.startsWith('mcq') || headerName.startsWith('fib') || headerName.startsWith('grid') || headerName.includes('n1') || headerName.includes('n2')) {
          acc.body = acc.body || {};
          acc.body[headerName] = cellValue;
        } else if (headerName.includes('l2_skill') || headerName.includes('l3_skill') || headerName.includes('sub_skill')) {
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
        } else if (headerName.includes('x+x')) {
          acc['sub_skill_xx'] = cellValue;
        } else if (headerName.includes('x+0')) {
          acc['sub_skill_x0'] = cellValue;
        } else if (headerName.includes('is_atomic')) {
          acc['is_atomic'] = cellValue.toLocaleString().toLowerCase() === 'true';
        } else if (headerName.includes('instruction_media')) {
          acc['instruction_media'] = typeof cellValue === 'string' ? [cellValue] : cellValue;
        } else if (headerName.includes('instruction_text')) {
          acc['instruction_text'] = cellValue;
        } else {
          acc[headerName] = cellValue;
        }
        acc.process_id = processId;
        acc.created_by = 'system';
        return acc;
      },
      {} as Record<string, any>,
    ),
  );
};

export const getUniqueValues = (data: any[]): UniqueValues => {
  const keys = ['l1_skill', 'l2_skill', 'l3_skill', 'board', 'class', 'sub_skills'];

  return _.reduce(
    keys,
    (acc: any, key) => {
      if (key === 'l2_skill' || key === 'l3_skill' || key === 'sub_skills') {
        acc[key] = _.uniq(_.flatten(data.map((item) => item[key] || []))).filter((value) => value !== undefined && value !== '');
      } else {
        acc[key] = _.uniqBy(data, key)
          .map((item) => item[key])
          .filter((value) => value !== undefined && value !== '');
      }
      return acc;
    },
    {},
  ) as UniqueValues;
};

export const checkValidity = async (data: any[]): Promise<{ error: { errStatus: string | null; errMsg: string | null }; result: { isValid: boolean; data: any } }> => {
  const uniqueValues: UniqueValues = getUniqueValues(data);
  const { boards, classes, skills, subSkills, repositories } = await preloadData();

  const mismatches: Mismatches = {
    boards: _.difference(
      uniqueValues.board,
      boards.flatMap((board: Board) => board.name.en),
    ),
    classes: _.difference(
      uniqueValues.class,
      classes.flatMap((Class: Class) => Class.name.en),
    ),
    repository: _.difference(
      uniqueValues.repository || [],
      repositories.flatMap((repo: Board) => repo.name.en),
    ),
    l1_skill: _.difference(
      uniqueValues.l1_skill,
      skills.filter((skill: Skill) => skill.type === 'l1_skill').map((skill: Skill) => skill.name.en),
    ),
    l2_skill: _.difference(
      uniqueValues.l2_skill,
      skills.filter((skill: Skill) => skill.type === 'l2_skill').map((skill: Skill) => skill.name.en),
    ),
    l3_skill: _.difference(
      _.flatMap(uniqueValues.l3_skill),
      skills.filter((skill: Skill) => skill.type === 'l3_skill').map((skill: Skill) => skill.name.en),
    ),
    sub_skills: _.difference(
      uniqueValues.sub_skills,
      subSkills.flatMap((Sub_skill: SubSkill) => Sub_skill.name.en),
    ),
  };

  const hasMismatch = _.some(_.values(mismatches), (arr) => arr.length > 0);

  if (hasMismatch) {
    const mismatchedFields = Object.entries(mismatches)
      .filter(([, mismatchArray]) => mismatchArray.length > 0)
      .map(([field, mismatchedArray]) => `${field}: ${mismatchedArray.join(', ')}`)
      .join('; ');
    logger.error(`One or more values do not match the preloaded data.${mismatchedFields}`);
    return {
      error: { errStatus: 'Mismatch', errMsg: `One or more values do not match the preloaded data.${mismatchedFields}` },
      result: { isValid: false, data: [mismatchedFields] },
    };
  }

  return {
    error: { errStatus: null, errMsg: null },
    result: { isValid: true, data: uniqueValues },
  };
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
