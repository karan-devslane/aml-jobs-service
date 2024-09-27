import { DataTypes } from 'sequelize';
import { AppDataSource } from '../config';

export const QuestionStage = AppDataSource.define(
  'question_stage',
  {
    id: {
      type: DataTypes.INTEGER,
      primaryKey: true,
      autoIncrement: true,
    },
    process_id: {
      type: DataTypes.UUID,
      allowNull: false,
    },
    question_text: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    question_id: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    question_set_id: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    sequence: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },
    question_type: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    repository_name: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    board: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    class: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    L1_skill: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    L2_skill: {
      type: DataTypes.ARRAY(DataTypes.STRING),
      allowNull: false,
    },
    L3_skill: {
      type: DataTypes.ARRAY(DataTypes.STRING),
      allowNull: true,
    },
    gradient: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    hint: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    description: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    body: {
      type: DataTypes.JSONB,
      allowNull: false,
    },
    benchmark_time: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },
    sub_skill: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    sub_skill_carry: {
      type: DataTypes.ARRAY(DataTypes.STRING),
      allowNull: true,
    },
    sub_skill_procedural: {
      type: DataTypes.ARRAY(DataTypes.STRING),
      allowNull: true,
    },
    sub_skill_x_plus_0: {
      type: DataTypes.ARRAY(DataTypes.STRING),
      allowNull: true,
    },
    sub_skill_x_plus_x: {
      type: DataTypes.ARRAY(DataTypes.STRING),
      allowNull: true,
    },
    media_files: {
      type: DataTypes.JSONB,
      allowNull: false,
    },
    status: {
      type: DataTypes.ENUM('progress', 'errored', 'success'),
      allowNull: true,
    },
    error_info: {
      type: DataTypes.JSON,
      allowNull: true,
    },
    created_by: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    updated_by: {
      type: DataTypes.STRING,
      allowNull: true,
    },
  },
  {
    tableName: 'question_stage',
    timestamps: true,
    createdAt: 'created_at',
    updatedAt: 'updated_at',
  },
);
