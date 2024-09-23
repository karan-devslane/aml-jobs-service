import { QuestionSet } from '../models/questionSet';

//create service for QuestionSet
export const createQuestionSet = async (insertData: Record<string, unknown>[]): Promise<any> => {
  try {
    await QuestionSet.bulkCreate(insertData);
    return { error: false, message: 'success' };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to create a record' : '';
    return { error: true, message: errorMsg };
  }
};
