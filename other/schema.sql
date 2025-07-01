/*
 Navicat Premium Data Transfer

 Source Server         : 172.32.6.155
 Source Server Type    : PostgreSQL
 Source Server Version : 140008 (140008)
 Source Host           : 172.32.6.155:7654
 Source Catalog        : data_sync
 Source Schema         : public

 Target Server Type    : PostgreSQL
 Target Server Version : 140008 (140008)
 File Encoding         : 65001

 Date: 10/02/2025 15:26:28
*/

-- ----------------------------
-- Table structure for subscription_info
-- ----------------------------
CREATE TABLE IF NOT EXISTS "public"."subscription_info"
(
    "pkid"                  varchar(64) COLLATE "pg_catalog"."default"  NOT NULL,
    "code"                  varchar(64) COLLATE "pg_catalog"."default"  NOT NULL,
    "name"                  varchar(255) COLLATE "pg_catalog"."default" NOT NULL,
    "user_full_sync_enable" bool         DEFAULT false,
    "schedule_enable"       bool         DEFAULT false,
    "schedule_type"         int4,
    "schedule_conf"         varchar(255) COLLATE "pg_catalog"."default",
    "custom_mapping_enable" bool,
    "create_time"           timestamp(6) DEFAULT CURRENT_TIMESTAMP,
    "create_user"           varchar(64) COLLATE "pg_catalog"."default",
    "update_time"           timestamp(6) DEFAULT CURRENT_TIMESTAMP,
    "update_user"           varchar(64) COLLATE "pg_catalog"."default",
    CONSTRAINT "code_unique" UNIQUE ("code"),
    CONSTRAINT "subscription_info_pkey" PRIMARY KEY ("pkid")
)
;
COMMENT ON COLUMN "public"."subscription_info"."pkid" IS '主键';
COMMENT ON COLUMN "public"."subscription_info"."code" IS '编码';
COMMENT ON COLUMN "public"."subscription_info"."name" IS '名称';
COMMENT ON COLUMN "public"."subscription_info"."user_full_sync_enable" IS '开启全量同步用户，false：指定同步，true：全量同步';
COMMENT ON COLUMN "public"."subscription_info"."schedule_enable" IS '开启定时同步，false：未开启，true：开启';
COMMENT ON COLUMN "public"."subscription_info"."schedule_type" IS '调度类型，1：CRON表达式，2：固定速度';
COMMENT ON COLUMN "public"."subscription_info"."schedule_conf" IS '调度配置';
COMMENT ON COLUMN "public"."subscription_info"."custom_mapping_enable" IS '开启自定义参数映射，false：未开启，true：开启';
COMMENT ON COLUMN "public"."subscription_info"."create_time" IS '创建时间';
COMMENT ON COLUMN "public"."subscription_info"."create_user" IS '创建人';
COMMENT ON COLUMN "public"."subscription_info"."update_time" IS '修改时间';
COMMENT ON COLUMN "public"."subscription_info"."update_user" IS '修改人';

-- ----------------------------
-- Uniques structure for table subscription_info
-- ----------------------------
COMMENT ON CONSTRAINT "code_unique" ON "public"."subscription_info" IS '编码唯一';

-- ----------------------------
-- Table structure for subscription_detail
-- ----------------------------
CREATE TABLE IF NOT EXISTS "public"."subscription_detail"
(
    "pkid"              varchar(64) COLLATE "pg_catalog"."default" NOT NULL,
    "subscription_code" varchar(64) COLLATE "pg_catalog"."default" NOT NULL,
    "service_type"      int4                                       NOT NULL,
    "config_json"       text COLLATE "pg_catalog"."default",
    "create_time"       timestamp(6) DEFAULT CURRENT_TIMESTAMP,
    "create_user"       varchar(64) COLLATE "pg_catalog"."default",
    "update_time"       timestamp(6) DEFAULT CURRENT_TIMESTAMP,
    "update_user"       varchar(64) COLLATE "pg_catalog"."default",
    CONSTRAINT "subscription_detail_pkey" PRIMARY KEY ("pkid")
)
;
COMMENT ON COLUMN "public"."subscription_detail"."pkid" IS '主键';
COMMENT ON COLUMN "public"."subscription_detail"."subscription_code" IS '订阅信息编码';
COMMENT ON COLUMN "public"."subscription_detail"."service_type" IS '业务类型，0：用户、1：部门、2：岗位、3：角色';
COMMENT ON COLUMN "public"."subscription_detail"."config_json" IS '配置信息';
COMMENT ON COLUMN "public"."subscription_detail"."create_time" IS '创建时间';
COMMENT ON COLUMN "public"."subscription_detail"."create_user" IS '创建人';
COMMENT ON COLUMN "public"."subscription_detail"."update_time" IS '修改时间';
COMMENT ON COLUMN "public"."subscription_detail"."update_user" IS '修改人';

-- ----------------------------
-- Table structure for sync_master
-- ----------------------------
CREATE TABLE IF NOT EXISTS "public"."sync_master"
(
    "pkid"           varchar(64) COLLATE "pg_catalog"."default" NOT NULL,
    "instance_id"    varchar(64) COLLATE "pg_catalog"."default" NOT NULL,
    "last_heartbeat" timestamp(6)                               NOT NULL,
    CONSTRAINT "sync_master_pkey" PRIMARY KEY ("pkid")
)
;
COMMENT ON COLUMN "public"."sync_master"."pkid" IS '主键';
COMMENT ON COLUMN "public"."sync_master"."instance_id" IS '实例ID';
COMMENT ON COLUMN "public"."sync_master"."last_heartbeat" IS '最后一次更新时间';
