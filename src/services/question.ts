import { Question } from '../models/question';

//create service for Question
export const createQuestion = async (insertData: Record<string, unknown>[]): Promise<any> => {
  try {
    const stagingData = await Question.bulkCreate(insertData);

    const [dataValues] = stagingData;
    return { error: false, message: 'success', dataValues };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to create a record' : '';
    return { error: true, message: errorMsg };
  }
};
