import { AppDataSource } from '../config';
import { QuestionSet } from '../models/questionSet';
import logger from '../utils/logger';

export const createQuestionSet = async (insertData: Array<Record<string, any>>): Promise<any> => {
  const transact = await AppDataSource.transaction();
  try {
    await QuestionSet.bulkCreate(insertData, { transaction: transact });
    await transact.commit();
    return { error: false, message: 'success' };
  } catch (error) {
    await transact.rollback();
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to create a record' : '';
    return { error: true, message: errorMsg };
  }
};
