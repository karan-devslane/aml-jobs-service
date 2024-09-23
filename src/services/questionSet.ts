import { QuestionSet } from '../models/questionSet';
import { Optional } from 'sequelize';

//create service for QuestionSet
export const createQuestionSet = async (req: Optional<any, any>[]): Promise<any> => {
  try {
    await QuestionSet.bulkCreate(req);
    return { error: false, message: 'success' };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to create a record' : '';
    return { error: true, message: errorMsg };
  }
};
