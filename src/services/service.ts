import { boardMaster } from '../models/boardMaster';
import { classMaster } from '../models/classMaster';
import { Repository } from '../models/repository';
import { SkillMaster } from '../models/skill';
import { SubSkillMaster } from '../models/subSkillMaster';
import { Tenant } from '../models/tenant';
import logger from '../utils/logger';

export const getTenants = async (): Promise<any> => {
  try {
    const tenants = await Tenant.findAll({
      where: { is_active: true },
      attributes: ['id', 'name'],
      raw: true,
    });
    return tenants;
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get records' : '';
    return { error: true, message: errorMsg };
  }
};

export const getBoards = async (): Promise<any> => {
  try {
    const boards = await boardMaster.findAll({
      where: { is_active: true },
      attributes: ['id', 'name'],
      raw: true,
    });
    return boards;
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get records' : '';
    return { error: true, message: errorMsg };
  }
};

export const getClasses = async (): Promise<any> => {
  try {
    const classes = await classMaster.findAll({
      where: { is_active: true },
      attributes: ['id', 'name'],
      raw: true,
    });
    return classes;
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get records' : '';
    return { error: true, message: errorMsg };
  }
};

export const getSkills = async (): Promise<any> => {
  try {
    const skills = await SkillMaster.findAll({
      where: { is_active: true },
      attributes: ['id', 'name', 'type'],
      raw: true,
    });
    return skills;
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get records' : '';
    return { error: true, message: errorMsg };
  }
};

export const getSubSkills = async (): Promise<any> => {
  try {
    const subSkills = await SubSkillMaster.findAll({
      where: { is_active: true },
      attributes: ['id', 'name'],
      raw: true,
    });
    return subSkills;
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get records' : '';
    return { error: true, message: errorMsg };
  }
};

export const getRepository = async (): Promise<any> => {
  try {
    const repositories = await Repository.findAll({
      where: { is_active: true },
      attributes: ['id', 'name'],
      raw: true,
    });
    return repositories;
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get records' : '';
    return { error: true, message: errorMsg };
  }
};
