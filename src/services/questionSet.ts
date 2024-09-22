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

export const getAllQuestionSet = async (): Promise<any> => {
  const QuestionSets = await QuestionSet.findAll({});
  const questionSets = QuestionSets.map((qs) => qs.dataValues);
  return questionSets;
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
