import { Question } from '../models/question';
import logger from '../utils/logger';

export const createQuestion = async (insertData: Array<Record<string, any>>): Promise<any> => {
  try {
    const stagingData = await Question.bulkCreate(insertData);
    const [dataValues] = stagingData;
    return { error: false, message: 'success', dataValues };
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to create a record' : '';
    return { error: true, message: errorMsg };
  }
};
