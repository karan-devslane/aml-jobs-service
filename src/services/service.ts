import { boardMaster } from '../models/boardMaster';
import { classMaster } from '../models/classMaster';
import { Repository } from '../models/repository';
import { SkillMaster } from '../models/skill';
import { SubSkillMaster } from '../models/subSkillMaster';
import { Tenant } from '../models/tenant';
import logger from '../utils/logger';

export const getTenants = async (): Promise<any> => {
  try {
    const Tenants = await Tenant.findAll({
      where: { is_active: true },
      attributes: { include: ['id', 'name'] },
    });

    const tenants = Tenants.map((tenant) => ({
      id: tenant.dataValues.id,
      name: JSON.parse(tenant.dataValues.name),
    }));

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
    const Boards = await boardMaster.findAll({
      where: { is_active: true },
      attributes: { include: ['id', 'name'] },
    });

    const boards = Boards.map((board) => ({
      id: board.dataValues.id,
      name: board.dataValues.name,
    }));
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
    const Classes = await classMaster.findAll({
      where: { is_active: true },
      attributes: { include: ['id', 'name'] },
    });

    const classes = Classes.map((c) => ({
      id: c.dataValues.id,
      name: c.dataValues.name,
    }));

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
    const Skills = await SkillMaster.findAll({
      where: { is_active: true },
      attributes: { include: ['id', 'name', 'type'] },
    });

    const skills = Skills.map((skill) => ({
      id: skill.dataValues.id,
      name: skill.dataValues.name,
      type: skill.dataValues.type,
    }));
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
    const SubSkills = await SubSkillMaster.findAll({
      where: { is_active: true },
      attributes: { include: ['id', 'name'] },
    });

    const subSkills = SubSkills.map((subSkill) => ({
      id: subSkill.dataValues.id,
      name: subSkill.dataValues.name,
    }));

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
    const Repositories = await Repository.findAll({
      where: { is_active: true },
      attributes: { include: ['id', 'name'] },
    });

    const repositories = Repositories.map((repo) => ({
      id: repo.dataValues.id,
      name: repo.dataValues.name,
    }));

    return repositories;
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get records' : '';
    return { error: true, message: errorMsg };
  }
};
