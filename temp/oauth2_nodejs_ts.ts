// package.json
{
  "name": "oauth2-server-configurable",
  "version": "1.0.0",
  "description": "Configurable OAuth 2.0 Server with Node.js and TypeScript",
  "main": "dist/index.js",
  "scripts": {
    "build": "tsc",
    "dev": "ts-node src/index.ts",
    "start": "node dist/index.js",
    "config:validate": "ts-node src/scripts/validate-config.ts"
  },
  "dependencies": {
    "express": "^4.18.2",
    "cors": "^2.8.5",
    "helmet": "^7.0.0",
    "dotenv": "^16.3.1",
    "axios": "^1.5.0",
    "jsonwebtoken": "^9.0.2",
    "crypto": "^1.0.1",
    "joi": "^17.9.2",
    "yaml": "^2.3.2",
    "express-rate-limit": "^6.8.1",
    "winston": "^3.10.0"
  },
  "devDependencies": {
    "@types/express": "^4.17.17",
    "@types/cors": "^2.8.13",
    "@types/jsonwebtoken": "^9.0.2",
    "@types/node": "^20.5.0",
    "typescript": "^5.1.6",
    "ts-node": "^10.9.1"
  }
}

// src/types/config.ts
export interface OAuthProviderConfig {
  name: string;
  clientId: string;
  clientSecret: string;
  authorizationUrl: string;
  tokenUrl: string;
  userInfoUrl?: string;
  scope?: string;
  redirectUri?: string;
  usePKCE?: boolean;
  additionalParams?: Record<string, string>;
  userMapping?: UserFieldMapping;
  enabled?: boolean;
}

export interface UserFieldMapping {
  id: string;
  email?: string;
  name?: string;
  avatar?: string;
  username?: string;
}

export interface ServerConfig {
  port: number;
  host: string;
  cors: {
    origins: string[];
    credentials: boolean;
  };
  jwt: {
    secret: string;
    expiresIn: string;
    issuer: string;
  };
  session: {
    stateExpiration: number; // minutes
    cookieSecret: string;
  };
  rateLimit: {
    windowMs: number;
    max: number;
  };
  logging: {
    level: string;
    format: string;
  };
  security: {
    helmet: boolean;
    httpsOnly: boolean;
  };
}

export interface AppConfig {
  server: ServerConfig;
  oauth: {
    providers: OAuthProviderConfig[];
    defaultRedirectUri: string;
    allowDynamicRedirect: boolean;
    maxStateAge: number;
  };
}

// src/config/schema.ts
import Joi from 'joi';

export const oauthProviderSchema = Joi.object({
  name: Joi.string().required(),
  clientId: Joi.string().required(),
  clientSecret: Joi.string().required(),
  authorizationUrl: Joi.string().uri().required(),
  tokenUrl: Joi.string().uri().required(),
  userInfoUrl: Joi.string().uri().optional(),
  scope: Joi.string().optional(),
  redirectUri: Joi.string().uri().optional(),
  usePKCE: Joi.boolean().default(false),
  additionalParams: Joi.object().optional(),
  userMapping: Joi.object({
    id: Joi.string().required(),
    email: Joi.string().optional(),
    name: Joi.string().optional(),
    avatar: Joi.string().optional(),
    username: Joi.string().optional()
  }).optional(),
  enabled: Joi.boolean().default(true)
});

export const configSchema = Joi.object({
  server: Joi.object({
    port: Joi.number().port().default(3000),
    host: Joi.string().default('localhost'),
    cors: Joi.object({
      origins: Joi.array().items(Joi.string()).default(['http://localhost:3000']),
      credentials: Joi.boolean().default(true)
    }),
    jwt: Joi.object({
      secret: Joi.string().min(32).required(),
      expiresIn: Joi.string().default('7d'),
      issuer: Joi.string().default('oauth2-server')
    }),
    session: Joi.object({
      stateExpiration: Joi.number().min(1).max(60).default(5),
      cookieSecret: Joi.string().min(32).required()
    }),
    rateLimit: Joi.object({
      windowMs: Joi.number().default(15 * 60 * 1000),
      max: Joi.number().default(100)
    }),
    logging: Joi.object({
      level: Joi.string().valid('error', 'warn', 'info', 'debug').default('info'),
      format: Joi.string().valid('json', 'simple').default('simple')
    }),
    security: Joi.object({
      helmet: Joi.boolean().default(true),
      httpsOnly: Joi.boolean().default(false)
    })
  }).required(),
  oauth: Joi.object({
    providers: Joi.array().items(oauthProviderSchema).min(1).required(),
    defaultRedirectUri: Joi.string().uri().required(),
    allowDynamicRedirect: Joi.boolean().default(true),
    maxStateAge: Joi.number().default(5 * 60 * 1000)
  }).required()
});

// src/config/loader.ts
import fs from 'fs';
import path from 'path';
import yaml from 'yaml';
import { AppConfig } from '../types/config';
import { configSchema } from './schema';
import { logger } from '../utils/logger';

export class ConfigLoader {
  private static instance: ConfigLoader;
  private config: AppConfig | null = null;

  private constructor() {}

  static getInstance(): ConfigLoader {
    if (!ConfigLoader.instance) {
      ConfigLoader.instance = new ConfigLoader();
    }
    return ConfigLoader.instance;
  }

  loadConfig(configPath?: string): AppConfig {
    if (this.config) {
      return this.config;
    }

    const configFile = configPath || process.env.CONFIG_FILE || './config/oauth.yaml';
    
    try {
      // 尝试加载配置文件
      const configContent = fs.readFileSync(configFile, 'utf8');
      let rawConfig: any;

      if (configFile.endsWith('.yaml') || configFile.endsWith('.yml')) {
        rawConfig = yaml.parse(configContent);
      } else if (configFile.endsWith('.json')) {
        rawConfig = JSON.parse(configContent);
      } else {
        throw new Error('Unsupported config file format. Use .yaml, .yml, or .json');
      }

      // 环境变量覆盖
      this.applyEnvironmentOverrides(rawConfig);

      // 验证配置
      const { error, value } = configSchema.validate(rawConfig, { allowUnknown: false });
      if (error) {
        throw new Error(`Config validation error: ${error.details[0].message}`);
      }

      this.config = value;
      logger.info('Configuration loaded successfully');
      return this.config;

    } catch (error) {
      logger.error('Failed to load configuration:', error);
      throw error;
    }
  }

  private applyEnvironmentOverrides(config: any): void {
    // 服务器配置覆盖
    if (process.env.PORT) config.server.port = parseInt(process.env.PORT);
    if (process.env.HOST) config.server.host = process.env.HOST;
    if (process.env.JWT_SECRET) config.server.jwt.secret = process.env.JWT_SECRET;
    if (process.env.COOKIE_SECRET) config.server.session.cookieSecret = process.env.COOKIE_SECRET;
    if (process.env.CORS_ORIGINS) {
      config.server.cors.origins = process.env.CORS_ORIGINS.split(',');
    }

    // OAuth 提供商环境变量覆盖
    config.oauth.providers.forEach((provider: any, index: number) => {
      const prefix = `OAUTH_${provider.name.toUpperCase()}`;
      if (process.env[`${prefix}_CLIENT_ID`]) {
        provider.clientId = process.env[`${prefix}_CLIENT_ID`];
      }
      if (process.env[`${prefix}_CLIENT_SECRET`]) {
        provider.clientSecret = process.env[`${prefix}_CLIENT_SECRET`];
      }
      if (process.env[`${prefix}_REDIRECT_URI`]) {
        provider.redirectUri = process.env[`${prefix}_REDIRECT_URI`];
      }
    });
  }

  getConfig(): AppConfig {
    if (!this.config) {
      throw new Error('Configuration not loaded. Call loadConfig() first.');
    }
    return this.config;
  }

  reloadConfig(): AppConfig {
    this.config = null;
    return this.loadConfig();
  }
}

// src/utils/logger.ts
import winston from 'winston';

export const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.simple()
    })
  ]
});

// src/services/provider.service.ts
import { OAuthProviderConfig, UserFieldMapping } from '../types/config';
import { ConfigLoader } from '../config/loader';
import { logger } from '../utils/logger';

export class ProviderService {
  private static instance: ProviderService;
  private providers: Map<string, OAuthProviderConfig> = new Map();

  private constructor() {
    this.loadProviders();
  }

  static getInstance(): ProviderService {
    if (!ProviderService.instance) {
      ProviderService.instance = new ProviderService();
    }
    return ProviderService.instance;
  }

  private loadProviders(): void {
    const config = ConfigLoader.getInstance().getConfig();
    
    config.oauth.providers.forEach(provider => {
      if (provider.enabled !== false) {
        this.providers.set(provider.name, provider);
        logger.info(`Loaded OAuth provider: ${provider.name}`);
      }
    });
  }

  getProvider(name: string): OAuthProviderConfig | undefined {
    return this.providers.get(name);
  }

  getAllProviders(): OAuthProviderConfig[] {
    return Array.from(this.providers.values());
  }

  getEnabledProviders(): OAuthProviderConfig[] {
    return Array.from(this.providers.values()).filter(p => p.enabled !== false);
  }

  addProvider(provider: OAuthProviderConfig): void {
    this.providers.set(provider.name, provider);
    logger.info(`Added OAuth provider: ${provider.name}`);
  }

  removeProvider(name: string): boolean {
    const result = this.providers.delete(name);
    if (result) {
      logger.info(`Removed OAuth provider: ${name}`);
    }
    return result;
  }

  updateProvider(name: string, updates: Partial<OAuthProviderConfig>): boolean {
    const provider = this.providers.get(name);
    if (!provider) {
      return false;
    }

    const updatedProvider = { ...provider, ...updates };
    this.providers.set(name, updatedProvider);
    logger.info(`Updated OAuth provider: ${name}`);
    return true;
  }

  mapUserData(providerName: string, userData: any): any {
    const provider = this.getProvider(providerName);
    if (!provider?.userMapping) {
      return userData;
    }

    const mapping = provider.userMapping;
    const mappedData: any = {};

    // 映射字段
    Object.entries(mapping).forEach(([targetField, sourceField]) => {
      if (sourceField && userData[sourceField] !== undefined) {
        mappedData[targetField] = userData[sourceField];
      }
    });

    // 保留原始数据
    return { ...userData, ...mappedData };
  }

  reloadProviders(): void {
    this.providers.clear();
    this.loadProviders();
  }
}

// src/services/oauth.service.ts
import axios from 'axios';
import { OAuthProviderConfig, UserFieldMapping } from '../types/config';
import { TokenResponse, UserInfo, AuthState } from '../types/oauth';
import { CryptoUtils } from '../utils/crypto';
import { ConfigLoader } from '../config/loader';
import { ProviderService } from './provider.service';
import { logger } from '../utils/logger';

export interface OAuthUrlOptions {
  usePKCE?: boolean;
  redirectTo?: string;
  additionalParams?: Record<string, string>;
  customRedirectUri?: string;
}

export class OAuthService {
  private provider: OAuthProviderConfig;
  private stateStore: Map<string, AuthState> = new Map();
  private config = ConfigLoader.getInstance().getConfig();

  constructor(providerName: string) {
    const provider = ProviderService.getInstance().getProvider(providerName);
    if (!provider) {
      throw new Error(`OAuth provider '${providerName}' not found or disabled`);
    }
    this.provider = provider;
  }

  generateAuthUrl(options: OAuthUrlOptions = {}): { url: string; state: string } {
    const state = CryptoUtils.generateState();
    const authState: AuthState = { state };

    // 确定重定向 URI
    const redirectUri = options.customRedirectUri || 
                       this.provider.redirectUri || 
                       this.config.oauth.defaultRedirectUri;

    const params = new URLSearchParams({
      client_id: this.provider.clientId,
      redirect_uri: redirectUri,
      response_type: 'code',
      state,
      ...this.provider.additionalParams,
      ...options.additionalParams
    });

    if (this.provider.scope) {
      params.append('scope', this.provider.scope);
    }

    // PKCE 支持
    const usePKCE = options.usePKCE ?? this.provider.usePKCE ?? false;
    if (usePKCE) {
      const codeVerifier = CryptoUtils.generateCodeVerifier();
      const codeChallenge = CryptoUtils.generateCodeChallenge(codeVerifier);
      
      authState.codeVerifier = codeVerifier;
      params.append('code_challenge', codeChallenge);
      params.append('code_challenge_method', 'S256');
    }

    if (options.redirectTo) {
      authState.redirectTo = options.redirectTo;
    }

    // 存储状态
    this.stateStore.set(state, authState);

    // 设置状态过期时间
    const expirationTime = this.config.server.session.stateExpiration * 60 * 1000;
    setTimeout(() => {
      this.stateStore.delete(state);
    }, expirationTime);

    const url = `${this.provider.authorizationUrl}?${params.toString()}`;
    
    logger.debug(`Generated auth URL for provider ${this.provider.name}`, {
      provider: this.provider.name,
      state,
      usePKCE
    });

    return { url, state };
  }

  async exchangeCodeForToken(code: string, state: string, redirectUri?: string): Promise<TokenResponse> {
    const authState = this.stateStore.get(state);
    if (!authState) {
      throw new Error('Invalid or expired state');
    }

    const finalRedirectUri = redirectUri || 
                           this.provider.redirectUri || 
                           this.config.oauth.defaultRedirectUri;

    const params = new URLSearchParams({
      client_id: this.provider.clientId,
      client_secret: this.provider.clientSecret,
      code,
      redirect_uri: finalRedirectUri,
      grant_type: 'authorization_code'
    });

    // PKCE 验证
    if (authState.codeVerifier) {
      params.append('code_verifier', authState.codeVerifier);
    }

    try {
      const response = await axios.post(this.provider.tokenUrl, params, {
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      });

      // 清理状态
      this.stateStore.delete(state);

      logger.info(`Successfully exchanged code for token`, {
        provider: this.provider.name,
        hasRefreshToken: !!response.data.refresh_token
      });

      return response.data;
    } catch (error) {
      logger.error('Token exchange failed', {
        provider: this.provider.name,
        error: error instanceof Error ? error.message : 'Unknown error'
      });
      throw new Error('Failed to exchange code for token');
    }
  }

  async getUserInfo(accessToken: string): Promise<UserInfo> {
    if (!this.provider.userInfoUrl) {
      throw new Error('User info URL not configured for this provider');
    }

    try {
      const response = await axios.get(this.provider.userInfoUrl, {
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Accept': 'application/json'
        }
      });

      // 使用提供商服务映射用户数据
      const mappedData = ProviderService.getInstance().mapUserData(
        this.provider.name, 
        response.data
      );

      logger.info(`Successfully fetched user info`, {
        provider: this.provider.name,
        userId: mappedData.id
      });

      return mappedData;
    } catch (error) {
      logger.error('Failed to fetch user info', {
        provider: this.provider.name,
        error: error instanceof Error ? error.message : 'Unknown error'
      });
      throw new Error('Failed to fetch user info');
    }
  }

  async refreshToken(refreshToken: string): Promise<TokenResponse> {
    const params = new URLSearchParams({
      client_id: this.provider.clientId,
      client_secret: this.provider.clientSecret,
      refresh_token: refreshToken,
      grant_type: 'refresh_token'
    });

    try {
      const response = await axios.post(this.provider.tokenUrl, params, {
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      });

      logger.info(`Successfully refreshed token`, {
        provider: this.provider.name
      });

      return response.data;
    } catch (error) {
      logger.error('Token refresh failed', {
        provider: this.provider.name,
        error: error instanceof Error ? error.message : 'Unknown error'
      });
      throw new Error('Failed to refresh token');
    }
  }

  getRedirectTarget(state: string): string | undefined {
    const authState = this.stateStore.get(state);
    return authState?.redirectTo;
  }

  getProviderInfo(): OAuthProviderConfig {
    return this.provider;
  }
}

// src/routes/config.routes.ts
import express from 'express';
import { ConfigLoader } from '../config/loader';
import { ProviderService } from '../services/provider.service';
import { oauthProviderSchema } from '../config/schema';
import { authMiddleware } from '../middleware/auth.middleware';

const router = express.Router();

// 获取服务器配置信息（公开信息）
router.get('/config/info', (req, res) => {
  const config = ConfigLoader.getInstance().getConfig();
  
  res.json({
    server: {
      version: process.env.npm_package_version || '1.0.0',
      environment: process.env.NODE_ENV || 'development'
    },
    oauth: {
      providers: ProviderService.getInstance().getEnabledProviders().map(p => ({
        name: p.name,
        enabled: p.enabled,
        supportsPKCE: p.usePKCE,
        scopes: p.scope?.split(' ') || []
      })),
      allowDynamicRedirect: config.oauth.allowDynamicRedirect
    }
  });
});

// 获取所有提供商配置 (需要认证)
router.get('/config/providers', authMiddleware, (req, res) => {
  const providers = ProviderService.getInstance().getAllProviders();
  
  // 隐藏敏感信息
  const safeProviders = providers.map(p => ({
    ...p,
    clientSecret: '***hidden***'
  }));

  res.json(safeProviders);
});

// 添加新的提供商配置
router.post('/config/providers', authMiddleware, (req, res) => {
  const { error, value } = oauthProviderSchema.validate(req.body);
  
  if (error) {
    return res.status(400).json({ 
      error: 'Invalid provider configuration', 
      details: error.details 
    });
  }

  try {
    ProviderService.getInstance().addProvider(value);
    res.json({ success: true, message: 'Provider added successfully' });
  } catch (error) {
    res.status(500).json({ error: 'Failed to add provider' });
  }
});

// 更新提供商配置
router.put('/config/providers/:name', authMiddleware, (req, res) => {
  const { name } = req.params;
  const { error, value } = oauthProviderSchema.validate(req.body);
  
  if (error) {
    return res.status(400).json({ 
      error: 'Invalid provider configuration', 
      details: error.details 
    });
  }

  const success = ProviderService.getInstance().updateProvider(name, value);
  
  if (success) {
    res.json({ success: true, message: 'Provider updated successfully' });
  } else {
    res.status(404).json({ error: 'Provider not found' });
  }
});

// 删除提供商配置
router.delete('/config/providers/:name', authMiddleware, (req, res) => {
  const { name } = req.params;
  const success = ProviderService.getInstance().removeProvider(name);
  
  if (success) {
    res.json({ success: true, message: 'Provider removed successfully' });
  } else {
    res.status(404).json({ error: 'Provider not found' });
  }
});

// 重新加载配置
router.post('/config/reload', authMiddleware, (req, res) => {
  try {
    ConfigLoader.getInstance().reloadConfig();
    ProviderService.getInstance().reloadProviders();
    res.json({ success: true, message: 'Configuration reloaded successfully' });
  } catch (error) {
    res.status(500).json({ error: 'Failed to reload configuration' });
  }
});

export default router;

// src/routes/auth.routes.ts
import express from 'express';
import jwt from 'jsonwebtoken';
import rateLimit from 'express-rate-limit';
import { OAuthService } from '../services/oauth.service';
import { ProviderService } from '../services/provider.service';
import { ConfigLoader } from '../config/loader';
import { logger } from '../utils/logger';

const router = express.Router();
const config = ConfigLoader.getInstance().getConfig();

// 速率限制
const authRateLimit = rateLimit({
  windowMs: config.server.rateLimit.windowMs,
  max: config.server.rateLimit.max,
  message: { error: 'Too many authentication attempts, please try again later' }
});

// 获取可用的提供商
router.get('/auth/providers', (req, res) => {
  const providers = ProviderService.getInstance().getEnabledProviders();
  
  res.json({
    providers: providers.map(p => ({
      name: p.name,
      supportsPKCE: p.usePKCE,
      scopes: p.scope?.split(' ') || []
    }))
  });
});

// 启动 OAuth 授权流程
router.get('/auth/:provider', authRateLimit, (req, res) => {
  const { provider } = req.params;
  const { redirect_to, use_pkce, custom_redirect_uri } = req.query;

  try {
    const oauthService = new OAuthService(provider);
    const { url, state } = oauthService.generateAuthUrl({
      usePKCE: use_pkce === 'true',
      redirectTo: redirect_to as string,
      customRedirectUri: custom_redirect_uri as string
    });

    // 将状态存储在安全的 Cookie 中
    res.cookie('oauth_state', state, {
      httpOnly: true,
      secure: config.server.security.httpsOnly,
      sameSite: 'lax',
      maxAge: config.server.session.stateExpiration * 60 * 1000
    });

    res.json({ 
      authUrl: url,
      provider: provider,
      state: state
    });

  } catch (error) {
    logger.error('Auth initiation failed', { provider, error });
    res.status(400).json({ 
      error: error instanceof Error ? error.message : 'Unknown error' 
    });
  }
});

// OAuth 回调处理
router.get('/auth/:provider/callback', async (req, res) => {
  const { provider } = req.params;
  const { code, state, error } = req.query;

  if (error) {
    logger.warn('OAuth callback error', { provider, error });
    return res.status(400).json({ error: `OAuth error: ${error}` });
  }

  const storedState = req.cookies?.oauth_state;
  if (!storedState || storedState !== state) {
    logger.warn('State mismatch in OAuth callback', { provider, storedState, receivedState: state });
    return res.status(400).json({ error: 'Invalid state parameter' });
  }

  try {
    const oauthService = new OAuthService(provider);
    
    // 交换授权码获取访问令牌
    const tokenResponse = await oauthService.exchangeCodeForToken(
      code as string,
      state as string
    );

    // 获取用户信息
    const userInfo = await oauthService.getUserInfo(tokenResponse.access_token);

    // 生成 JWT 令牌
    const jwtToken = jwt.sign(
      {
        id: userInfo.id,
        email: userInfo.email,
        name: userInfo.name,
        provider,
        iat: Math.floor(Date.now() / 1000)
      },
      config.server.jwt.secret,
      { 
        expiresIn: config.server.jwt.expiresIn,
        issuer: config.server.jwt.issuer
      }
    );

    // 清理状态 Cookie
    res.clearCookie('oauth_state');

    // 获取重定向目标
    const redirectTo = oauthService.getRedirectTarget(state as string);

    logger.info('OAuth authentication successful', {
      provider,
      userId: userInfo.id,
      email: userInfo.email
    });

    res.json({
      success: true,
      token: jwtToken,
      user: {
        id: userInfo.id,
        email: userInfo.email,
        name: userInfo.name,
        avatar: userInfo.avatar,
        provider
      },
      redirectTo: redirectTo || '/',
      expiresIn: config.server.jwt.expiresIn
    });

  } catch (error) {
    logger.error('OAuth callback processing failed', { provider, error });
    res.status(500).json({ error: 'Authentication failed' });
  }
});

// 刷新令牌
router.post('/auth/refresh', async (req, res) => {
  const { refresh_token, provider } = req.body;

  if (!refresh_token || !provider) {
    return res.status(400).json({ error: 'Missing refresh token or provider' });
  }

  try {
    const oauthService = new OAuthService(provider);
    const tokenResponse = await oauthService.refreshToken(refresh_token);

    res.json({
      success: true,
      access_token: tokenResponse.access_token,
      expires_in: tokenResponse.expires_in,
      token_type: tokenResponse.token_type
    });
  } catch (error) {
    logger.error('Token refresh failed', { provider, error });
    res.status(500).json({ error: 'Failed to refresh token' });
  }
});

export default router;

// src/index.ts
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import cookieParser from 'cookie-parser';
import rateLimit from 'express-rate-limit';
import dotenv from 'dotenv';

import { ConfigLoader } from './config/loader';
import { ProviderService } from './services/provider.service';
import { logger } from './utils/logger';

import authRoutes from './routes/auth.routes';
import configRoutes from './routes/config.routes';
import protectedRoutes from './routes/protected.routes';

// 加载环境变量
dotenv.config();

// 初始化应用配置
const configLoader = ConfigLoader.getInstance();
const config = configLoader.loadConfig();

// 初始化提供商服务
const providerService = ProviderService.getInstance();

const app = express();

// 安全中间件
if (config.server.security.helmet) {
  app.use(helmet());
}

// CORS 配置
app.use(cors({
  origin: config.server.cors.origins,
  credentials: config.server.cors.credentials
}));

// 全局速率限制
app.use(rateLimit({
  windowMs: config.server.rateLimit.windowMs,
  max: config.server.rateLimit.max,
  message: { error: 'Too many requests, please try again later' }
}));

// 基础中间件
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(cookieParser(config.server.session.cookieSecret));

// 日志中间件
app.use((req, res, next) => {
  logger.info('Request received', {
    method: req.method,
    url: req.url,
    ip: req.ip,
    userAgent: req.get('User-Agent')
  });
  next();
});

// 路由
app.use('/api', authRoutes);
app.use('/api', configRoutes);
app.use('/api', protectedRoutes);

// 健康检查
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    version: process.env.npm_package_version || '1.0.0',
    providers: providerService.getEnabledProviders().length
  });
});

// 服务器信息
app.get('/info', (req, res) => {
  res.json({
    name: 'OAuth 2.0 Server',
    version: process.env.npm_package_version || '1.0.0',
    environment: process.env.NODE_ENV || 'development',
    uptime: process.uptime(),
    providers: providerService.getEnabledProviders().map(p => p.name)
  });
});

// 错误处理中间件
app.use((err: Error, req: express.Request, res: express.Response, next: express.NextFunction) => {
  logger.error('Unhandled error', {
    error: err.message,
    stack: err.stack,
    url: req.url,
    method: req.method
  });
  
  res.status(500).json({ 
    error: 'Internal server error',
    timestamp: new Date().toISOString()
  });
});

// 404 处理
app.use((req, res) => {
  res.status(404).json({ 
    error: 'Route not found',
    path: req.path,
    method: req.method
  });
});

// 启动服务器
const server = app.listen(config.server.port, config.server.host, () => {
  logger.info(`OAuth 2.0 Server is running`, {
    host: config.server.host,
    port: config.server.port,
    environment: process.env.NODE_ENV || 'development',
    providers: providerService.getEnabledProviders().map(p => p.name)
  });
});

// 优雅关闭
process.on('SIGTERM', () => {
  logger.info('SIGTERM received, shutting down gracefully');
  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  logger.info('SIGINT received, shutting down gracefully');
  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });
});

// src/types/oauth.ts
export interface TokenResponse {
  access_token: string;
  token_type: string;
  expires_in: number;
  refresh_token?: string;
  scope?: string;
}

export interface UserInfo {
  id: string;
  email?: string;
  name?: string;
  avatar?: string;
  username?: string;
  [key: string]: any;
}

export interface AuthState {
  state: string;
  codeVerifier?: string;
  nonce?: string;
  redirectTo?: string;
}

// src/utils/crypto.ts
import crypto from 'crypto';

export class CryptoUtils {
  static generateRandomString(length: number = 32): string {
    return crypto.randomBytes(length).toString('hex');
  }

  static generateCodeVerifier(): string {
    return crypto.randomBytes(32).toString('base64url');
  }

  static generateCodeChallenge(codeVerifier: string): string {
    return crypto.createHash('sha256').update(codeVerifier).digest('base64url');
  }

  static generateState(): string {
    return this.generateRandomString(16);
  }
}

// src/middleware/auth.middleware.ts
import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import { ConfigLoader } from '../config/loader';
import { logger } from '../utils/logger';

export interface AuthRequest extends Request {
  user?: any;
}

export const authMiddleware = (req: AuthRequest, res: Response, next: NextFunction) => {
  const token = req.header('Authorization')?.replace('Bearer ', '');
  const config = ConfigLoader.getInstance().getConfig();

  if (!token) {
    return res.status(401).json({ error: 'Access denied. No token provided.' });
  }

  try {
    const decoded = jwt.verify(token, config.server.jwt.secret);
    req.user = decoded;
    
    logger.debug('Token validated successfully', {
      userId: (decoded as any).id,
      provider: (decoded as any).provider
    });
    
    next();
  } catch (error) {
    logger.warn('Invalid token provided', { error });
    res.status(400).json({ error: 'Invalid token.' });
  }
};

// src/routes/protected.routes.ts
import express from 'express';
import { authMiddleware, AuthRequest } from '../middleware/auth.middleware';

const router = express.Router();

// 受保护的路由示例
router.get('/profile', authMiddleware, (req: AuthRequest, res) => {
  res.json({
    message: 'Protected route accessed successfully',
    user: req.user
  });
});

router.get('/dashboard', authMiddleware, (req: AuthRequest, res) => {
  res.json({
    message: 'Welcome to your dashboard',
    user: req.user,
    timestamp: new Date().toISOString()
  });
});

export default router;

// src/scripts/validate-config.ts
import { ConfigLoader } from '../config/loader';
import { logger } from '../utils/logger';

async function validateConfig() {
  try {
    const configLoader = ConfigLoader.getInstance();
    const config = configLoader.loadConfig();
    
    console.log('✅ Configuration validation successful');
    console.log(`✅ Server will run on ${config.server.host}:${config.server.port}`);
    console.log(`✅ Found ${config.oauth.providers.length} OAuth providers`);
    
    config.oauth.providers.forEach(provider => {
      console.log(`  - ${provider.name}: ${provider.enabled !== false ? 'enabled' : 'disabled'}`);
    });
    
    process.exit(0);
  } catch (error) {
    console.error('❌ Configuration validation failed:', error);
    process.exit(1);
  }
}

if (require.main === module) {
  validateConfig();
}

// config/oauth.yaml - 示例配置文件
/*
server:
  port: 3000
  host: "0.0.0.0"
  cors:
    origins:
      - "http://localhost:3000"
      - "https://yourdomain.com"
    credentials: true
  jwt:
    secret: "your-super-secret-jwt-key-minimum-32-characters-long"
    expiresIn: "7d"
    issuer: "oauth2-server"
  session:
    stateExpiration: 5  # minutes
    cookieSecret: "your-cookie-secret-minimum-32-characters-long"
  rateLimit:
    windowMs: 900000  # 15 minutes
    max: 100
  logging:
    level: "info"
    format: "simple"
  security:
    helmet: true
    httpsOnly: false

oauth:
  defaultRedirectUri: "http://localhost:3000/auth/callback"
  allowDynamicRedirect: true
  maxStateAge: 300000  # 5 minutes
  providers:
    - name: "github"
      clientId: "your-github-client-id"
      clientSecret: "your-github-client-secret"
      authorizationUrl: "https://github.com/login/oauth/authorize"
      tokenUrl: "https://github.com/login/oauth/access_token"
      userInfoUrl: "https://api.github.com/user"
      scope: "user:email"
      usePKCE: false
      enabled: true
      userMapping:
        id: "id"
        email: "email"
        name: "name"
        avatar: "avatar_url"
        username: "login"
    
    - name: "google"
      clientId: "your-google-client-id"
      clientSecret: "your-google-client-secret"
      authorizationUrl: "https://accounts.google.com/o/oauth2/v2/auth"
      tokenUrl: "https://oauth2.googleapis.com/token"
      userInfoUrl: "https://www.googleapis.com/oauth2/v2/userinfo"
      scope: "openid email profile"
      usePKCE: true
      enabled: true
      userMapping:
        id: "id"
        email: "email"
        name: "name"
        avatar: "picture"
    
    - name: "microsoft"
      clientId: "your-microsoft-client-id"
      clientSecret: "your-microsoft-client-secret"
      authorizationUrl: "https://login.microsoftonline.com/common/oauth2/v2.0/authorize"
      tokenUrl: "https://login.microsoftonline.com/common/oauth2/v2.0/token"
      userInfoUrl: "https://graph.microsoft.com/v1.0/me"
      scope: "openid email profile"
      usePKCE: true
      enabled: true
      userMapping:
        id: "id"
        email: "mail"
        name: "displayName"
        username: "userPrincipalName"
*/

// .env 示例文件
/*
NODE_ENV=development
CONFIG_FILE=./config/oauth.yaml
LOG_LEVEL=info

# JWT 密钥 (覆盖配置文件)
JWT_SECRET=your-super-secret-jwt-key-minimum-32-characters-long
COOKIE_SECRET=your-cookie-secret-minimum-32-characters-long

# 服务器配置
PORT=3000
HOST=0.0.0.0
CORS_ORIGINS=http://localhost:3000,https://yourdomain.com

# OAuth 提供商配置 (覆盖配置文件)
OAUTH_GITHUB_CLIENT_ID=your-github-client-id
OAUTH_GITHUB_CLIENT_SECRET=your-github-client-secret
OAUTH_GITHUB_REDIRECT_URI=http://localhost:3000/auth/github/callback

OAUTH_GOOGLE_CLIENT_ID=your-google-client-id
OAUTH_GOOGLE_CLIENT_SECRET=your-google-client-secret
OAUTH_GOOGLE_REDIRECT_URI=http://localhost:3000/auth/google/callback

OAUTH_MICROSOFT_CLIENT_ID=your-microsoft-client-id
OAUTH_MICROSOFT_CLIENT_SECRET=your-microsoft-client-secret
OAUTH_MICROSOFT_REDIRECT_URI=http://localhost:3000/auth/microsoft/callback
*/

// Dockerfile
/*
FROM node:18-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production

COPY . .
RUN npm run build

EXPOSE 3000

USER node

CMD ["npm", "start"]
*/

// docker-compose.yml
/*
version: '3.8'

services:
  oauth2-server:
    build: .
    ports:
      - "3000:3000"
    environment:
      - NODE_ENV=production
      - CONFIG_FILE=/app/config/oauth.yaml
    volumes:
      - ./config:/app/config:ro
      - ./logs:/app/logs
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:3000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
*/

// README.md
/*
# OAuth 2.0 Server with TypeScript

A configurable OAuth 2.0 server implementation with Node.js and TypeScript.

## Features

- 🔧 **Configurable**: YAML/JSON configuration with environment variable overrides
- 🔐 **Multi-Provider**: Support for GitHub, Google, Microsoft, and custom providers
- 🛡️ **Security**: PKCE, state validation, rate limiting, JWT tokens
- 📝 **Logging**: Structured logging with Winston
- 🚀 **Production Ready**: Docker support, health checks, graceful shutdown
- 🔄 **Hot Reload**: Dynamic configuration reloading
- 🎯 **Type Safe**: Full TypeScript support

## Quick Start

1. Install dependencies:
```bash
npm install
```

2. Copy and configure:
```bash
cp config/oauth.yaml.example config/oauth.yaml
cp .env.example .env
```

3. Update your OAuth provider credentials in the config file

4. Run the server:
```bash
npm run dev
```

## Configuration

Configuration is loaded from:
1. YAML/JSON configuration file
2. Environment variables (override config file)

### Environment Variables

OAuth provider credentials can be set via environment variables:
- `OAUTH_GITHUB_CLIENT_ID`
- `OAUTH_GITHUB_CLIENT_SECRET`
- `OAUTH_GOOGLE_CLIENT_ID`
- `OAUTH_GOOGLE_CLIENT_SECRET`

## API Endpoints

### Authentication
- `GET /api/auth/providers` - List available providers
- `GET /api/auth/:provider` - Start OAuth flow
- `GET /api/auth/:provider/callback` - OAuth callback
- `POST /api/auth/refresh` - Refresh tokens

### Configuration (Protected)
- `GET /api/config/info` - Public server info
- `GET /api/config/providers` - List providers (admin)
- `POST /api/config/providers` - Add provider (admin)
- `PUT /api/config/providers/:name` - Update provider (admin)
- `DELETE /api/config/providers/:name` - Remove provider (admin)
- `POST /api/config/reload` - Reload configuration (admin)

### Protected Routes
- `GET /api/profile` - User profile
- `GET /api/dashboard` - User dashboard

## Usage Examples

### Start OAuth Flow
```javascript
const response = await fetch('/api/auth/github?use_pkce=true');
const { authUrl } = await response.json();
window.location.href = authUrl;
```

### Add New Provider
```javascript
const provider = {
  name: 'custom',
  clientId: 'your-client-id',
  clientSecret: 'your-client-secret',
  authorizationUrl: 'https://example.com/oauth/authorize',
  tokenUrl: 'https://example.com/oauth/token',
  userInfoUrl: 'https://example.com/api/user',
  scope: 'read:user',
  usePKCE: true,
  userMapping: {
    id: 'id',
    email: 'email',
    name: 'full_name'
  }
};

await fetch('/api/config/providers', {
  method: 'POST',
  headers: { 
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}`
  },
  body: JSON.stringify(provider)
});
```

## Docker Deployment

```bash
docker-compose up -d
```

## Development

```bash
npm run dev          # Development server
npm run build        # Build for production
npm run config:validate # Validate configuration
```
*/