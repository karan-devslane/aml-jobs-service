import logger from '../utils/logger';
import * as _ from 'lodash';
import * as uuid from 'uuid';
import { uploadMediaFile } from '../services/awsService';
import { updateProcess } from '../services/process';
import { createQuestionStage, getAllStageQuestion, questionStageMetaData, updateQuestionStage } from '../services/questionStage';
import { QuestionStage } from '../models/questionStage';
import { appConfiguration } from '../config';
import { createQuestion } from '../services/question';
import { getCSVTemplateHeader, getCSVHeaderAndRow, validateHeader, processRow, convertToCSV, preloadData, checkValidity } from '../services/util';
import { Status } from '../enums/status';
import { getQuestionSets } from '../services/questionSet';

let mediaFileEntries: any[];
let processId: string;

const { grid1AddFields, grid1DivFields, grid1MultipleFields, grid1SubFields, grid2Fields, mcqFields, fibFields } = appConfiguration;

export const handleQuestionCsv = async (questionsCsv: object[], media: any, process_id: string) => {
  processId = process_id;
  mediaFileEntries = media;
  let questionsData: object[] = [];
  if (questionsCsv.length === 0) {
    logger.error(`${processId} Question data validation resulted in empty data.`);
    return {
      error: { errStatus: 'Empty', errMsg: 'empty question data found' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }

  for (const questions of questionsCsv) {
    const validQuestionHeader = await validateCSVQuestionHeaderRow(questions);
    if (!validQuestionHeader.result.isValid) return validQuestionHeader;
    const {
      result: { data },
    } = validQuestionHeader;

    const validQuestionRows = processQuestionRows(data?.rows, data?.header);
    if (!validQuestionRows.result.isValid) return validQuestionRows;
    const { result } = validQuestionRows;

    questionsData = questionsData.concat(result.data);
    if (questionsData.length === 0) {
      logger.error('Error while processing the question csv data');
      return {
        error: { errStatus: 'Empty', errMsg: 'empty question data found' },
        result: {
          isValid: false,
          data: null,
        },
      };
    }
  }

  logger.info('Insert question Stage::Questions Data ready for bulk insert');
  const createQuestions = await bulkInsertQuestionStage(questionsData);
  if (!createQuestions.result.isValid) return createQuestions;

  const validateQuestions = await validateStagedQuestionData();
  if (!validateQuestions.result.isValid) {
    const uploadQuestion = await uploadErroredQuestionsToCloud();
    if (!uploadQuestion.result.isValid) return uploadQuestion;
    return validateQuestions;
  }

  await updateProcess(processId, { status: Status.VALIDATED });

  const questionsMedia = await processQuestionMediaFiles();
  if (!questionsMedia.result.isValid) {
    logger.error('Error while validating stage question table');
    return questionsMedia;
  }
  const insertedMainQuestions = await insertMainQuestions();
  return insertedMainQuestions;
};

const validateCSVQuestionHeaderRow = async (questionEntry: any) => {
  const templateHeader = await getCSVTemplateHeader(questionEntry.entryName);
  if (!templateHeader.result.isValid) {
    return {
      error: { errStatus: 'Template missing', errMsg: 'template missing' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  const questionRowHeader = getCSVHeaderAndRow(questionEntry);
  if (!questionRowHeader.result.isValid) {
    logger.error(`Question Row/header::Template header, header, or rows are missing  for file ${questionEntry.entryName}`);
    return questionRowHeader;
  }
  const {
    result: {
      data: { header },
    },
  } = questionRowHeader;
  const isValidHeader = validateHeader(questionEntry.entryName, header, templateHeader.result.data);
  if (!isValidHeader.result.isValid) {
    logger.error(isValidHeader.error.errMsg);
    return isValidHeader;
  }
  logger.info(`Question Row/header::Row and Header mapping process started for ${processId} `);
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: questionRowHeader.result.data,
    },
  };
};

const processQuestionRows = (rows: any, header: any) => {
  const processData = processRow(rows, header);
  if (!processData || processData.length === 0) {
    logger.error('Question Row/header:: Row processing failed or returned empty data');
    return {
      error: { errStatus: 'process_error', errMsg: 'Question Row/header:: Row processing failed or returned empty data' },
      result: {
        isValid: false,
        data: processData,
      },
    };
  }
  logger.info('Question Row/header:: header and row process successfully and process 2 started');
  const updatedProcessData = processQuestionStage(processData);
  if (!updatedProcessData || updatedProcessData.length === 0) {
    logger.error('Question Row/header:: Stage 2 data processing failed or returned empty data');
    return {
      error: { errStatus: 'process_stage_error', errMsg: 'Data processing failed or returned empty data' },
      result: {
        isValid: false,
        data: updatedProcessData,
      },
    };
  }
  logger.info('Insert question Stage::Questions Data ready for bulk insert');
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: updatedProcessData,
    },
  };
};

const bulkInsertQuestionStage = async (insertData: object[]) => {
  const questionStage = await createQuestionStage(insertData);
  if (questionStage.error) {
    logger.error(`Insert Staging:: ${processId} question bulk data error in inserting ${questionStage.message}`);
    return {
      error: { errStatus: 'errored', errMsg: questionStage.message },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  logger.info(`Insert Question Staging:: ${processId} question bulk data inserted successfully to staging table`);
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: null,
    },
  };
};

const validateStagedQuestionData = async () => {
  const getAllQuestionStage = await questionStageMetaData({ process_id: processId });
  const validateMetadata = await checkValidity(getAllQuestionStage);
  if (!validateMetadata.result.isValid) return validateMetadata;
  if (getAllQuestionStage.error) {
    logger.error(`Validate Question Stage:: ${processId}.`);
    return {
      error: { errStatus: 'error', errMsg: `Validate Question Stage:: ${processId}.` },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  let isUnique = true;
  let isValid = true;
  let errStatus = null,
    errMsg = null;
  if (_.isEmpty(getAllQuestionStage)) {
    logger.error(`Validate Question Stage:: ${processId} ,the question Data is empty,`);
    return {
      error: { errStatus: 'error', errMsg: `the question Data is empty` },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  for (const question of getAllQuestionStage) {
    const { id, question_id, question_set_id, question_type, l1_skill, body } = question;
    const checkRecord = await questionStageMetaData({ question_id, question_set_id, l1_skill, question_type });
    if (checkRecord.error) {
      logger.error(`Validate Question Stage:: ${processId} ,${checkRecord.message}.`);
      return {
        error: { errStatus: 'error', errMsg: `unexpected error ,${checkRecord.message}` },
        result: {
          isValid: false,
        },
      };
    }
    if (checkRecord.length > 1) {
      logger.error(`Duplicate question and question_set_id combination found for question id ${question_id} and question set id ${question_set_id} for ${question_type} ${l1_skill},`);
      await updateQuestionStage(
        { id },
        {
          status: 'errored',
          error_info: `Duplicate question and question_set_id combination found for question id ${question_id} and question set id ${question_set_id} for ${question_type} ${l1_skill},`,
        },
      );
      errStatus = 'errored';
      errMsg = `Duplicate question and question_set_id combination found for question id ${question_id} and question set id ${question_set_id} for ${question_type} ${l1_skill},`;
      isUnique = false;
    }
    let requiredFields: string[] = [];
    let requiredData;
    const caseKey = question_type === 'Grid-1' ? `${question_type}_${l1_skill}` : question_type;
    switch (caseKey) {
      case `Grid-1_add`:
        requiredFields = grid1AddFields;
        break;
      case `Grid-1_sub`:
        requiredFields = grid1SubFields;
        break;
      case `Grid-1_multiple`:
        requiredFields = grid1MultipleFields;
        break;
      case `Grid-1_division`:
        requiredFields = grid1DivFields;
        break;
      case `Grid-2`:
        requiredFields = grid2Fields;
        break;
      case `Mcq`:
        requiredFields = mcqFields;
        requiredData = 'question_text,mcq_question_image,mcq_option_1,mcq_option_2,mcq_option_3,mcq_option_4,mcq_option_5,mcq_option_6,mcq_correct_options';
        break;
      case `Fib`:
        requiredFields = fibFields;
        break;
      default:
        requiredFields = [];
        break;
    }
    if (!requiredFields.map((field) => body[field] !== undefined && body[field] !== null)) {
      requiredData = 'grid_fib_n1,grid_fib_n2';
      await updateQuestionStage(
        { id },
        {
          status: 'errored',
          error_info: `Missing required data for type ${question_type},fields are ${requiredData}`,
        },
      );
      errStatus = 'errored';
      errMsg = `Missing required data for type ${question_type},fields are ${requiredData}`;
      isValid = false;
    }
  }
  logger.info(`Validate Question Stage::${processId} , everything in the Question stage Data valid.`);
  return {
    error: { errStatus: errStatus, errMsg: errMsg },
    result: {
      isValid: isUnique && isValid,
    },
  };
};

const uploadErroredQuestionsToCloud = async () => {
  const getQuestions = await getAllStageQuestion();
  if (getQuestions.error) {
    logger.error('unexpected error occurred while get all stage data');
    return {
      error: { errStatus: 'unexpected_error', errMsg: 'unexpected error occurred while get all stage data' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  await updateProcess(processId, { question_error_file_name: 'questions.csv', status: Status.ERROR });
  const uploadQuestion = await convertToCSV(getQuestions, 'questions');
  if (!uploadQuestion) {
    logger.error('Upload Cloud::Unexpected error occurred while upload to cloud');
    return {
      error: { errStatus: 'unexpected_error', errMsg: 'unexpected error occurred while upload to cloud' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  logger.info('Question Upload Cloud::All the question are validated and uploaded in the cloud for reference');
  logger.info(`Question Media upload:: ${processId} question Stage data is ready for upload media `);
  return {
    error: { errStatus: 'validation_errored', errMsg: 'question file validation errored' },
    result: {
      isValid: true,
      data: null,
    },
  };
};

const processQuestionMediaFiles = async () => {
  try {
    const getQuestions = await getAllStageQuestion();
    if (getQuestions.error) {
      logger.error('unexpected error occurred while get all stage data');
      return {
        error: { errStatus: 'unexpected_error', errMsg: 'unexpected error occurred while get all stage data' },
        result: {
          isValid: false,
          data: null,
        },
      };
    }
    for (const question of getQuestions) {
      if (question.media_files?.length > 0) {
        const mediaFiles = await Promise.all(
          question.media_files.map(async (o: string) => {
            const foundMedia = mediaFileEntries.slice(1).find((media: any) => {
              return media.entryName.split('/')[1] === o;
            });
            if (foundMedia) {
              const mediaData = await uploadMediaFile(foundMedia, 'question');
              if (!mediaData) {
                logger.error(`Media upload failed for ${o}`);
                return null;
              }
              return mediaData;
            }
            return null;
          }),
        );
        if (mediaFiles.every((file: any) => file === null)) continue;

        const validMediaFiles = mediaFiles.filter((file) => file !== null);
        if (validMediaFiles.length === 0) {
          return {
            error: { errStatus: 'Empty', errMsg: 'No media found for the question' },
            result: {
              isValid: false,
              data: null,
            },
          };
        }
        const updateContent = await updateQuestionStage({ id: question.id }, { media_files: validMediaFiles });
        if (updateContent.error) {
          logger.error('Question Media upload:: Media validation failed');
          throw new Error('error while updating media');
        }
      }
    }

    logger.info('Question Media upload::inserted and updated in the process data');
    return {
      error: { errStatus: null, errMsg: null },
      result: {
        isValid: true,
        data: null,
      },
    };
  } catch (error: any) {
    logger.error(`An error occurred in processQuestionMediaFiles: ${error.message}`);
    return {
      error: { errStatus: 'process_error', errMsg: error.message },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
};

const insertMainQuestions = async () => {
  const insertToMainQuestion = await migrateToMainQuestion();
  if (!insertToMainQuestion.result.isValid) return insertToMainQuestion;

  logger.info(`Question Bulk insert:: bulk upload completed  for Process ID: ${processId}`);
  await QuestionStage.truncate({ restartIdentity: true });
  logger.info(`Completed:: ${processId} Question csv uploaded successfully`);
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: null,
    },
  };
};

export const migrateToMainQuestion = async () => {
  const getAllQuestionStage = await questionStageMetaData({ process_id: processId });
  if (getAllQuestionStage.error) {
    logger.error(`Validate Question Stage:: ${processId}.`);
    return {
      error: { errStatus: 'errored', errMsg: 'error while get all stage data' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  const insertData = await formatQuestionStageData(getAllQuestionStage);
  if (insertData.length === 0) {
    return {
      error: { errStatus: 'process_stage_data', errMsg: 'Error in formatting staging data to main table.' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  const questionInsert = await createQuestion(insertData);
  if (questionInsert.error) {
    logger.error(`Insert Question main:: ${processId} question bulk data error in inserting to main table.`);
    return {
      error: { errStatus: 'errored', errMsg: 'error while inserting staging data to question table' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }

  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: null,
    },
  };
};

const processQuestionStage = (questionsData: any) => {
  const fieldMapping: any = {
    'Grid-1_Addition': [...grid1AddFields, 'grid1_pre_fills_top', 'grid1_pre_fills_result'],
    'Grid-1_Subtraction': [...grid1SubFields, 'grid1_pre_fills_top', 'grid1_pre_fills_result'],
    'Grid-1_Multiplication': [...grid1MultipleFields, 'grid1_multiply_intermediate_steps_prefills', 'grid1_pre_fills_result'],
    'Grid-1_Division': [...grid1DivFields, 'grid1_pre_fills_remainder', 'grid1_pre_fills_quotient', 'grid1_div_intermediate_steps_prefills'],
    'Grid-2': [...grid2Fields, 'grid2_pre_fills_n1', 'grid2_pre_fills_n2'],
    Mcq: mcqFields,
    Fib: fibFields,
  };
  questionsData.forEach((question: any) => {
    const questionType = question.question_type === 'Grid-1' ? `${question.question_type}_${question.l1_skill}` : question.question_type;
    const relevantFields = fieldMapping[questionType];
    const filteredBody: any = {};
    relevantFields.forEach((field: any) => {
      if (question.body[field] !== undefined) {
        filteredBody[field] = question.body[field];
      }
    });
    question.body = filteredBody;
  });
  return questionsData;
};

const formatQuestionStageData = async (stageData: any[]) => {
  const { boards, classes, skills, subSkills, repositories } = await preloadData();
  const questionSetData = await getQuestionSets();

  const transformedData = stageData.map((obj) => {
    const {
      grid_fib_n1 = null,
      grid_fib_n2 = null,
      mcq_option_1 = null,
      mcq_option_2 = null,
      mcq_option_3 = null,
      mcq_option_4 = null,
      mcq_option_5 = null,
      mcq_option_6 = null,
      mcq_correct_options = null,
      sub_skill_carry = null,
      sub_skill_procedural = null,
      sub_skill_xx = null,
      sub_skill_x0 = null,
    } = obj.body || {};

    const questionSetId = questionSetData.find((qs: any) => qs.question_set_id === obj.question_set_id && qs.l1_skill === obj.l1_skill) || { identifier: null };
    const transferData = {
      identifier: uuid.v4(),
      question_set_id: questionSetId.identifier,
      question_type: obj.question_type,
      operation: obj.l1_skill,
      hints: obj.hint,
      sequence: obj.sequence,
      name: { en: obj.title || obj.question_text },
      description: { en: obj.description },
      tenant: '',
      repository: repositories.find((repository: any) => repository.name.en === obj.repository_name),
      taxonomy: {
        board: boards.find((board: any) => board.name.en === obj.board),
        class: classes.find((Class: any) => Class.name.en === obj.class),
        l1_skill: skills.find((skill: any) => skill.name.en == obj.l1_skill),
        l2_skill: obj.l2_skill?.map((skill: string) => skills.find((Skill: any) => Skill.name.en === skill)),
        l3_skill: obj.l3_skill?.map((skill: string) => skills.find((Skill: any) => Skill.name.en === skill)),
      },
      sub_skills: obj.sub_skills?.map((subSkill: string) => subSkills.find((sub: any) => sub.name.en === subSkill)),
      question_body: {
        numbers: { n1: grid_fib_n1, n2: grid_fib_n2 },
        options: obj.type === 'Mcq' ? [mcq_option_1, mcq_option_2, mcq_option_3, mcq_option_4, mcq_option_5, mcq_option_6] : undefined,
        correct_option: obj.type === 'Mcq' ? mcq_correct_options : undefined,
        answers: getAnswer(obj.l1_skill, grid_fib_n1, grid_fib_n2, obj.question_type, obj.body, obj.question_type),
        wrong_answer: convertWrongAnswerSubSkills({ sub_skill_carry, sub_skill_procedural, sub_skill_xx, sub_skill_x0 }),
      },
      benchmark_time: obj.benchmark_time,
      status: 'draft',
      media: obj.media_files,
      process_id: obj.process_id,
      created_by: 'system',
      is_active: true,
    };
    return transferData;
  });

  logger.info('Data transfer:: staging Data transferred as per original format');
  return transformedData;
};

const convertWrongAnswerSubSkills = (inputData: any) => {
  const wrongAnswers = [];

  for (const [key, value] of Object.entries(inputData)) {
    if (_.isEmpty(value)) {
      continue;
    }
    const numbers = (value as number[]).map(Number).filter((n: any) => !isNaN(n) && n !== 0);
    if (numbers.length > 0) {
      wrongAnswers.push({
        value: numbers,
        sub_skill_id: 1,
        subskillname: key,
      });
    }
  }
  logger.info('Wrong answer:: wrong answer mapped to sub skills');
  return wrongAnswers;
};

const getAnswer = (skill: string, num1: string, num2: string, type: string, bodyObject: any, question_type: string) => {
  switch (`${skill}_${question_type}`) {
    case 'Multiplication_Grid-1':
      return multiplyWithSteps(num1, num2, type, bodyObject);
    case 'Division_Grid-1':
      return divideWithSteps(Number(num2), Number(num1), type, bodyObject);

    case 'Addition_Grid-1':
      return addSubAnswer(bodyObject, skill);

    case 'Subtraction_Grid-1':
      return addSubAnswer(bodyObject, skill);

    default:
      return undefined;
  }
};

const addSubAnswer = (input: any, l1_skill: string) => {
  const { grid_fib_n1, grid_fib_n2, grid1_pre_fills_top, grid1_pre_fills_result, grid1_show_carry, grid1_show_regroup } = input;

  const n1Str = grid_fib_n1.padStart(Math.max(grid_fib_n1.length, grid_fib_n2.length), '0');
  const n2Str = grid_fib_n2.padStart(n1Str.length, '0');

  let result = 0;
  let answerTop = '';
  let answerResult = '';
  let isPrefil;

  if (l1_skill === 'Addition') {
    result = parseInt(n1Str) + parseInt(n2Str);
    isPrefil = grid1_show_carry === 'yes' ? true : false;
  } else if (l1_skill === 'Subtraction') {
    result = parseInt(n1Str) - parseInt(n2Str);
    isPrefil = grid1_show_regroup === 'yes' ? true : false;
  }

  const resultStr = result.toString().padStart(n1Str.length, '0');

  const finalPrefillTop = isPrefil ? grid1_pre_fills_top + 'B'.repeat(n1Str.length - grid1_pre_fills_top.length) : 'B'.repeat(n1Str.length);

  const updatedPrefilResult = grid1_pre_fills_result + 'B'.repeat(resultStr.length - grid1_pre_fills_result.length);

  for (let i = resultStr.length - 1; i >= 0; i--) {
    if (updatedPrefilResult[i] === 'B') {
      answerResult += 'B';
    } else {
      answerResult += resultStr[i];
    }
  }

  if (isPrefil && l1_skill === 'Addition') {
    let carry = 0;
    for (let i = n1Str.length - 1; i >= 0; i--) {
      const sum = parseInt(n1Str[i]) + parseInt(n2Str[i]) + carry;
      carry = Math.floor(sum / 10);

      if (finalPrefillTop[i] === 'B') {
        answerTop = 'B' + answerTop;
      } else {
        answerTop = carry.toString() + answerTop;
      }
    }
  } else if (isPrefil && l1_skill === 'Subtraction') {
    let borrow = 0;
    for (let i = n1Str.length - 1; i >= 0; i--) {
      let n1Digit = parseInt(n1Str[i]);
      const n2Digit = parseInt(n2Str[i]) + borrow;

      if (n1Digit < n2Digit) {
        borrow = 1;
        n1Digit += 10;
      } else {
        borrow = 0;
      }

      const difference = n1Digit - n2Digit;
      if (finalPrefillTop[i] === 'B') {
        answerTop = 'B' + answerTop;
      } else {
        answerTop = difference.toString();
      }
    }
  } else {
    answerTop = 'B'.repeat(n1Str.length);
  }
  return {
    result: parseInt(resultStr),
    answerTop,
    answerResult: answerResult.split('').reverse().join(''),
  };
};

const multiplyWithSteps = (num1: string, num2: string, type: string, prefillPattern?: any) => {
  const { grid1_multiply_intermediate_steps_prefills, grid1_pre_fills_result } = prefillPattern;
  const n1 = Number(num1);
  const n2 = Number(num2);
  if (type === 'Grid-1') {
    const num2Str = num2.toString();
    const num2Length = num2Str.length;
    let intermediateStep = '';
    let runningTotal = 0;
    let answerResult = '';
    let answerIntermediateResult = '';

    for (let i = 0; i < num2Length; i++) {
      const placeValue = parseInt(num2Str[num2Length - 1 - i]) * Math.pow(10, i);
      const product = n1 * placeValue;
      intermediateStep += product.toString();
      runningTotal += product;
    }

    const runningTotalAsString = runningTotal.toString();
    if (grid1_pre_fills_result.includes('F')) {
      const updatedPrefilResult = grid1_pre_fills_result + 'B'.repeat(runningTotalAsString.length - grid1_pre_fills_result.length);
      for (let i = runningTotalAsString.length - 1; i >= 0; i--) {
        if (updatedPrefilResult[i] === 'B') {
          answerResult += 'B';
        } else {
          answerResult += runningTotalAsString.toString()[i];
        }
      }
    }
    if (grid1_multiply_intermediate_steps_prefills.includes('F')) {
      const updatedIntermediatePrefilResult = grid1_multiply_intermediate_steps_prefills + 'B'.repeat(intermediateStep.length - grid1_multiply_intermediate_steps_prefills.length);
      for (let i = intermediateStep.length - 1; i >= 0; i--) {
        if (updatedIntermediatePrefilResult[i] === 'B') {
          answerIntermediateResult += 'B';
        } else {
          answerIntermediateResult += intermediateStep[i];
        }
      }
    }

    return {
      intermediateStep: intermediateStep,
      result: runningTotal,
      intermediatePrefil: answerIntermediateResult,
      answerPrefil: answerResult,
    };
  }
  return { result: n1 * n2 };
};

const divideWithSteps = (dividend: number, divisor: number, type: string, prefillPattern: any) => {
  const strDividend = dividend.toString();
  let quotient = '';
  let remainder = 0;
  let intermediateSteps = '';
  let prefilledQuotient;
  let prefilledRemainder;

  if (type === 'Grid-1') {
    const { grid1_pre_fills_quotient, grid1_pre_fills_remainder, grid1_div_intermediate_steps_prefills } = prefillPattern;

    if (divisor === 0) {
      throw new Error('Division by zero is not allowed.');
    }

    for (let i = 0; i < strDividend.length; i++) {
      const currentDigit = +strDividend[i];
      const currentNumber = remainder * 10 + currentDigit;
      const currentQuotient = Math.floor(currentNumber / divisor);
      remainder = currentNumber % divisor;
      quotient += currentQuotient;
      intermediateSteps += currentNumber;
    }
    quotient = quotient.replace(/^0+/, '') || '0';
    if (grid1_pre_fills_quotient || grid1_pre_fills_remainder || grid1_div_intermediate_steps_prefills) {
      if (grid1_pre_fills_quotient.includes('F')) {
        prefilledQuotient = applyPrefillPattern(quotient.toString(), grid1_pre_fills_quotient);
        quotient = prefilledQuotient;
      }
      if (grid1_pre_fills_remainder.includes('F')) {
        prefilledRemainder = applyPrefillPattern(remainder.toString(), grid1_pre_fills_remainder);
        remainder = parseInt(prefilledRemainder);
      }
      if (grid1_div_intermediate_steps_prefills.includes('F')) {
        intermediateSteps = applyPrefillPattern(intermediateSteps, grid1_div_intermediate_steps_prefills);
      }
    }

    return {
      quotient: quotient,
      remainder: remainder,
      intermediateSteps: intermediateSteps,
    };
  }
};

const applyPrefillPattern = (numStr: string, pattern: string) => {
  let result = '';
  const patternLength = pattern.length;

  for (let i = numStr.length - 1; i >= 0; i--) {
    if (i < patternLength) {
      const char = pattern[i] === 'B' ? 'B' : numStr[i];
      result += char;
    } else {
      result += 'B';
    }
  }
  return result;
};
