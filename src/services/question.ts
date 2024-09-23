import { Question } from '../models/question';
import { Optional } from 'sequelize';

//create service for Question
export const createQuestion = async (req: Optional<any, string>[]): Promise<any> => {
  try {
    const stagingData = await Question.bulkCreate(req);

    const [dataValues] = stagingData;
    return { error: false, message: 'success', dataValues };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to create a record' : '';
    return { error: true, message: errorMsg };
  }
};
