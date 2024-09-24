import { DataTypes } from 'sequelize';
import { AppDataSource } from '../config';

export const SkillMaster = AppDataSource.define(
  'skill_master',
  {
    id: {
      type: DataTypes.INTEGER,
      primaryKey: true,
      autoIncrement: true,
    },
    identifier: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    name: {
      type: DataTypes.JSONB,
      allowNull: false,
      comment: 'Multilingual field for skill name',
    },
    description: {
      type: DataTypes.STRING,
      allowNull: true,
      comment: 'A short description of the skill',
    },
    type: {
      type: DataTypes.ENUM('l1_skill', 'l2_skill', 'l3_skill'), // Enum field for type
      allowNull: false,
      comment: 'The type/category of the skill',
    },
    is_active: {
      type: DataTypes.BOOLEAN,
      allowNull: false,
      defaultValue: true,
      comment: 'Indicates whether the skill is active or not',
    },
    status: {
      type: DataTypes.ENUM('draft', 'live'),
      allowNull: false,
      defaultValue: 'draft',
      comment: 'The status of the skill',
    },
    created_by: {
      type: DataTypes.STRING,
      allowNull: false,
      comment: 'The user who created this skill',
    },
    updated_by: {
      type: DataTypes.STRING,
      allowNull: true,
      comment: 'The user who last updated this skill',
    },
  },
  {
    tableName: 'skill_master',
    timestamps: true,
    createdAt: 'created_at',
    updatedAt: 'updated_at',
    comment: 'Table to store skill-related data',
  },
);
