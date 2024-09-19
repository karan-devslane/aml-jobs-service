import { QuestionSet } from '../models/questionSet';
import { Optional } from 'sequelize';

//create service for QuestionSet
export const createQuestionSet = async (req: Optional<any, string>[]): Promise<any> => {
  try {
    const stagingData = await QuestionSet.bulkCreate(req);

    const [dataValues] = stagingData;
    return { error: false, message: 'success', dataValues };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to create a record' : '';
    return { error: true, message: errorMsg };
  }
};

//get Single QuestionSet by meta data
export const getAllQuestionSet = async (req: any): Promise<any> => {
  try {
    const questionSet = await QuestionSet.findAll({ where: req });
    return { questionSet };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};

//get Single QuestionSet by id
export const getQuestionSetById = async (id: number): Promise<any> => {
  try {
    const getQuestionSet = await QuestionSet.findOne({ where: { id } });
    return { error: false, getQuestionSet };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};
