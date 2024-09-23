import { QuestionSet } from '../models/questionSet';
import logger from '../utils/logger';

export const createQuestionSet = async (insertData: Array<Record<string, any>>): Promise<any> => {
  try {
    await QuestionSet.bulkCreate(insertData);
    return { error: false, message: 'success' };
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to create a record' : '';
    return { error: true, message: errorMsg };
  }
};
