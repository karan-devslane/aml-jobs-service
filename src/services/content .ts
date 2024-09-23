import { Content } from '../models/content';
import logger from '../utils/logger';

export const createContent = async (insertData: Array<Record<string, any>>): Promise<any> => {
  try {
    const insertContent = await Content.bulkCreate(insertData);
    return { insertContent };
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to create a record' : '';
    return { error: true, message: errorMsg };
  }
};
