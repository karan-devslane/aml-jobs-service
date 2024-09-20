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

//get Single Question by meta data
export const getAllQuestion = async (): Promise<any> => {
  try {
    const Questions = await Question.findAll({});
    const questions = Questions.map((q) => q.dataValues);
    return questions;
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};

//get Single Question by id
export const getQuestionById = async (id: number): Promise<any> => {
  try {
    const getQuestion = await Question.findOne({ where: { id } });
    return { error: false, getQuestion };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};
