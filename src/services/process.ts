import { Process } from '../models/process';
import logger from '../utils/logger';

export const getProcessMetaData = async (whereClause: any): Promise<any> => {
  try {
    whereClause.is_active = true;
    const getAllProcess = await Process.findAll({ where: whereClause, raw: true });
    return { error: false, getAllProcess };
  } catch (error) {
    logger.error('Error:: while execute the find all process');
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};

export const updateProcess = async (process_id: string, updateObj: any): Promise<any> => {
  try {
    const whereClause: Record<string, any> = { process_id };
    whereClause.is_active = true;
    const updateProcess = await Process.update(updateObj, { where: whereClause });

    return { error: false, updateProcess };
  } catch (error) {
    logger.error('Error:: while execute the update process');
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to update a record' : '';
    return { error: true, message: errorMsg };
  }
};
