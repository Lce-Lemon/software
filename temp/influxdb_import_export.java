// 1. 依赖配置 (pom.xml)
/*
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-data-jpa</artifactId>
    </dependency>
    <dependency>
        <groupId>com.influxdb</groupId>
        <artifactId>influxdb-client-java</artifactId>
        <version>6.10.0</version>
    </dependency>
    <dependency>
        <groupId>com.opencsv</groupId>
        <artifactId>opencsv</artifactId>
        <version>5.7.1</version>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-validation</artifactId>
    </dependency>
</dependencies>
*/

// 2. 配置类
package com.example.influxdb.config;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class InfluxDBConfig {

    @Value("${influxdb.url}")
    private String influxUrl;

    @Value("${influxdb.token}")
    private String influxToken;

    @Value("${influxdb.org}")
    private String influxOrg;

    @Value("${influxdb.bucket}")
    private String influxBucket;

    @Bean
    public InfluxDBClient influxDBClient() {
        return InfluxDBClientFactory.create(influxUrl, influxToken.toCharArray(), influxOrg, influxBucket);
    }

    @Bean(name = "taskExecutor")
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("async-");
        executor.initialize();
        return executor;
    }
}

// 3. 数据传输对象
package com.example.influxdb.dto;

import java.time.Instant;
import java.util.Map;

public class TimeSeriesData {
    private Instant time;
    private Map<String, String> tags;
    private Map<String, Object> fields;

    // 构造函数
    public TimeSeriesData() {}

    public TimeSeriesData(Instant time, Map<String, String> tags, Map<String, Object> fields) {
        this.time = time;
        this.tags = tags;
        this.fields = fields;
    }

    // Getters and Setters
    public Instant getTime() { return time; }
    public void setTime(Instant time) { this.time = time; }
    
    public Map<String, String> getTags() { return tags; }
    public void setTags(Map<String, String> tags) { this.tags = tags; }
    
    public Map<String, Object> getFields() { return fields; }
    public void setFields(Map<String, Object> fields) { this.fields = fields; }
}

// 4. 任务状态实体
package com.example.influxdb.entity;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "task_status")
public class TaskStatus {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "task_id")
    private String taskId;

    @Column(name = "task_type")
    private String taskType;

    @Column(name = "status")
    private String status;

    @Column(name = "progress")
    private Integer progress;

    @Column(name = "total_records")
    private Long totalRecords;

    @Column(name = "processed_records")
    private Long processedRecords;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "created_time")
    private LocalDateTime createdTime;

    @Column(name = "updated_time")
    private LocalDateTime updatedTime;

    // 构造函数
    public TaskStatus() {}

    public TaskStatus(String taskId, String taskType, String status) {
        this.taskId = taskId;
        this.taskType = taskType;
        this.status = status;
        this.progress = 0;
        this.processedRecords = 0L;
        this.createdTime = LocalDateTime.now();
        this.updatedTime = LocalDateTime.now();
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }

    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Integer getProgress() { return progress; }
    public void setProgress(Integer progress) { this.progress = progress; }

    public Long getTotalRecords() { return totalRecords; }
    public void setTotalRecords(Long totalRecords) { this.totalRecords = totalRecords; }

    public Long getProcessedRecords() { return processedRecords; }
    public void setProcessedRecords(Long processedRecords) { this.processedRecords = processedRecords; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public LocalDateTime getCreatedTime() { return createdTime; }
    public void setCreatedTime(LocalDateTime createdTime) { this.createdTime = createdTime; }

    public LocalDateTime getUpdatedTime() { return updatedTime; }
    public void setUpdatedTime(LocalDateTime updatedTime) { this.updatedTime = updatedTime; }
}

// 5. Repository
package com.example.influxdb.repository;

import com.example.influxdb.entity.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface TaskStatusRepository extends JpaRepository<TaskStatus, Long> {
    Optional<TaskStatus> findByTaskId(String taskId);
}

// 6. CSV处理服务
package com.example.influxdb.service;

import com.example.influxdb.dto.TimeSeriesData;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

@Service
public class CSVProcessingService {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int BATCH_SIZE = 1000;

    /**
     * 大文件流式读取CSV
     */
    public Stream<List<TimeSeriesData>> readLargeCSV(InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        CSVReader csvReader = new CSVReader(reader);
        
        // 读取表头
        String[] headers = csvReader.readNext();
        if (headers == null || headers.length < 2) {
            throw new IllegalArgumentException("CSV文件格式错误，至少需要time列和一个数据列");
        }

        return Stream.generate(() -> {
            try {
                List<TimeSeriesData> batch = new ArrayList<>();
                String[] line;
                int count = 0;
                
                while (count < BATCH_SIZE && (line = csvReader.readNext()) != null) {
                    TimeSeriesData data = parseCSVLine(headers, line);
                    if (data != null) {
                        batch.add(data);
                        count++;
                    }
                }
                
                return batch.isEmpty() ? null : batch;
            } catch (Exception e) {
                throw new RuntimeException("读取CSV文件失败", e);
            }
        }).takeWhile(Objects::nonNull).onClose(() -> {
            try {
                csvReader.close();
                reader.close();
            } catch (IOException e) {
                // 记录日志
            }
        });
    }

    /**
     * 解析CSV行数据
     */
    private TimeSeriesData parseCSVLine(String[] headers, String[] values) {
        if (values.length != headers.length) {
            return null; // 跳过格式错误的行
        }

        try {
            // 解析时间
            LocalDateTime dateTime = LocalDateTime.parse(values[0], DATE_FORMAT);
            Instant time = dateTime.toInstant(ZoneOffset.UTC);

            // 解析字段
            Map<String, Object> fields = new HashMap<>();
            Map<String, String> tags = new HashMap<>();

            for (int i = 1; i < headers.length; i++) {
                String header = headers[i].trim();
                String value = values[i].trim();
                
                if (!value.isEmpty()) {
                    // 尝试解析为数字，否则作为字符串标签
                    try {
                        if (value.contains(".")) {
                            fields.put(header, Double.parseDouble(value));
                        } else {
                            fields.put(header, Long.parseLong(value));
                        }
                    } catch (NumberFormatException e) {
                        tags.put(header, value);
                    }
                }
            }

            return new TimeSeriesData(time, tags, fields);
        } catch (Exception e) {
            return null; // 跳过解析失败的行
        }
    }

    /**
     * 写入CSV文件
     */
    public void writeCSV(List<TimeSeriesData> dataList, OutputStream outputStream) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        CSVWriter csvWriter = new CSVWriter(writer);

        if (!dataList.isEmpty()) {
            // 写入表头
            Set<String> allFields = new LinkedHashSet<>();
            allFields.add("time");
            
            for (TimeSeriesData data : dataList) {
                if (data.getTags() != null) {
                    allFields.addAll(data.getTags().keySet());
                }
                if (data.getFields() != null) {
                    allFields.addAll(data.getFields().keySet());
                }
            }

            String[] headers = allFields.toArray(new String[0]);
            csvWriter.writeNext(headers);

            // 写入数据
            for (TimeSeriesData data : dataList) {
                String[] row = new String[headers.length];
                row[0] = DATE_FORMAT.format(LocalDateTime.ofInstant(data.getTime(), ZoneOffset.UTC));

                for (int i = 1; i < headers.length; i++) {
                    String field = headers[i];
                    Object value = null;

                    if (data.getTags() != null && data.getTags().containsKey(field)) {
                        value = data.getTags().get(field);
                    } else if (data.getFields() != null && data.getFields().containsKey(field)) {
                        value = data.getFields().get(field);
                    }

                    row[i] = value != null ? value.toString() : "";
                }
                csvWriter.writeNext(row);
            }
        }

        csvWriter.close();
        writer.close();
    }
}

// 7. InfluxDB操作服务
package com.example.influxdb.service;

import com.example.influxdb.dto.TimeSeriesData;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class InfluxDBService {

    @Autowired
    private InfluxDBClient influxDBClient;

    @Value("${influxdb.bucket}")
    private String bucket;

    @Value("${influxdb.measurement:sensor_data}")
    private String measurement;

    /**
     * 批量写入数据到InfluxDB
     */
    public void writeBatch(List<TimeSeriesData> dataList) {
        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
        List<Point> points = new ArrayList<>();

        for (TimeSeriesData data : dataList) {
            Point point = Point.measurement(measurement)
                    .time(data.getTime(), WritePrecision.S);

            // 添加标签
            if (data.getTags() != null) {
                for (Map.Entry<String, String> tag : data.getTags().entrySet()) {
                    point.addTag(tag.getKey(), tag.getValue());
                }
            }

            // 添加字段
            if (data.getFields() != null) {
                for (Map.Entry<String, Object> field : data.getFields().entrySet()) {
                    if (field.getValue() instanceof Number) {
                        point.addField(field.getKey(), (Number) field.getValue());
                    } else {
                        point.addField(field.getKey(), field.getValue().toString());
                    }
                }
            }

            points.add(point);
        }

        writeApi.writePoints(bucket, influxDBClient.getOrg(), points);
    }

    /**
     * 按时间范围查询数据
     */
    public List<TimeSeriesData> queryByTimeRange(Instant startTime, Instant endTime) {
        QueryApi queryApi = influxDBClient.getQueryApi();
        
        String flux = String.format(
                "from(bucket: \"%s\") " +
                "|> range(start: %s, stop: %s) " +
                "|> filter(fn: (r) => r._measurement == \"%s\")",
                bucket,
                startTime.toString(),
                endTime.toString(),
                measurement
        );

        List<FluxTable> tables = queryApi.query(flux);
        List<TimeSeriesData> results = new ArrayList<>();
        Map<String, TimeSeriesData> dataMap = new HashMap<>();

        for (FluxTable table : tables) {
            for (FluxRecord record : table.getRecords()) {
                String timeKey = record.getTime().toString();
                TimeSeriesData data = dataMap.get(timeKey);
                
                if (data == null) {
                    data = new TimeSeriesData();
                    data.setTime(record.getTime());
                    data.setTags(new HashMap<>());
                    data.setFields(new HashMap<>());
                    dataMap.put(timeKey, data);
                }

                // 添加字段值
                String field = record.getField();
                Object value = record.getValue();
                data.getFields().put(field, value);

                // 添加标签
                record.getValues().forEach((k, v) -> {
                    if (k.startsWith("tag_") || (!k.equals("_time") && !k.equals("_value") && !k.equals("_field") && !k.equals("_measurement"))) {
                        data.getTags().put(k, v.toString());
                    }
                });
            }
        }

        results.addAll(dataMap.values());
        return results;
    }

    /**
     * 获取时间范围内的数据总数
     */
    public long countByTimeRange(Instant startTime, Instant endTime) {
        QueryApi queryApi = influxDBClient.getQueryApi();
        
        String flux = String.format(
                "from(bucket: \"%s\") " +
                "|> range(start: %s, stop: %s) " +
                "|> filter(fn: (r) => r._measurement == \"%s\") " +
                "|> count()",
                bucket,
                startTime.toString(),
                endTime.toString(),
                measurement
        );

        List<FluxTable> tables = queryApi.query(flux);
        return tables.stream()
                .flatMap(table -> table.getRecords().stream())
                .mapToLong(record -> (Long) record.getValue())
                .sum();
    }
}

// 8. 任务状态服务
package com.example.influxdb.service;

import com.example.influxdb.entity.TaskStatus;
import com.example.influxdb.repository.TaskStatusRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Service
public class TaskStatusService {

    @Autowired
    private TaskStatusRepository taskStatusRepository;

    public String createTask(String taskType) {
        String taskId = UUID.randomUUID().toString();
        TaskStatus taskStatus = new TaskStatus(taskId, taskType, "CREATED");
        taskStatusRepository.save(taskStatus);
        return taskId;
    }

    public void updateTaskStatus(String taskId, String status, Long processedRecords, Long totalRecords, String errorMessage) {
        Optional<TaskStatus> optionalTask = taskStatusRepository.findByTaskId(taskId);
        if (optionalTask.isPresent()) {
            TaskStatus task = optionalTask.get();
            task.setStatus(status);
            task.setUpdatedTime(LocalDateTime.now());
            
            if (processedRecords != null) {
                task.setProcessedRecords(processedRecords);
            }
            if (totalRecords != null) {
                task.setTotalRecords(totalRecords);
            }
            if (errorMessage != null) {
                task.setErrorMessage(errorMessage);
            }
            
            // 计算进度
            if (task.getTotalRecords() != null && task.getTotalRecords() > 0) {
                int progress = (int) ((task.getProcessedRecords() * 100) / task.getTotalRecords());
                task.setProgress(progress);
            }
            
            taskStatusRepository.save(task);
        }
    }

    public Optional<TaskStatus> getTaskStatus(String taskId) {
        return taskStatusRepository.findByTaskId(taskId);
    }
}

// 9. 导入导出请求DTO
package com.example.influxdb.dto;

import javax.validation.constraints.NotNull;
import java.time.Instant;

public class ExportRequest {
    @NotNull
    private Instant startTime;
    
    @NotNull
    private Instant endTime;
    
    private Long maxRecordsPerFile = 100000L; // 默认每个文件最大10万条记录

    // Getters and Setters
    public Instant getStartTime() { return startTime; }
    public void setStartTime(Instant startTime) { this.startTime = startTime; }

    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant endTime) { this.endTime = endTime; }

    public Long getMaxRecordsPerFile() { return maxRecordsPerFile; }
    public void setMaxRecordsPerFile(Long maxRecordsPerFile) { this.maxRecordsPerFile = maxRecordsPerFile; }
}

// 10. 异步导入导出服务
package com.example.influxdb.service;

import com.example.influxdb.dto.ExportRequest;
import com.example.influxdb.dto.TimeSeriesData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Service
public class AsyncImportExportService {

    private static final Logger logger = LoggerFactory.getLogger(AsyncImportExportService.class);
    
    @Autowired
    private CSVProcessingService csvProcessingService;
    
    @Autowired
    private InfluxDBService influxDBService;
    
    @Autowired
    private TaskStatusService taskStatusService;

    /**
     * 异步导入大文件
     */
    @Async("taskExecutor")
    public CompletableFuture<Void> importLargeFile(String taskId, MultipartFile file) {
        logger.info("开始异步导入任务，taskId: {}, 文件名: {}, 文件大小: {}", 
                   taskId, file.getOriginalFilename(), file.getSize());
        
        try {
            taskStatusService.updateTaskStatus(taskId, "RUNNING", null, null, null);
            
            long totalProcessed = 0;
            long batchCount = 0;
            
            try (InputStream inputStream = file.getInputStream();
                 Stream<List<TimeSeriesData>> dataStream = csvProcessingService.readLargeCSV(inputStream)) {
                
                for (List<TimeSeriesData> batch : dataStream.toArray(List[]::new)) {
                    if (batch != null && !batch.isEmpty()) {
                        // 写入InfluxDB
                        influxDBService.writeBatch(batch);
                        totalProcessed += batch.size();
                        batchCount++;
                        
                        // 更新任务进度（每100批次更新一次）
                        if (batchCount % 100 == 0) {
                            taskStatusService.updateTaskStatus(taskId, "RUNNING", totalProcessed, null, null);
                            logger.info("任务 {} 已处理 {} 条记录", taskId, totalProcessed);
                        }
                    }
                }
            }
            
            taskStatusService.updateTaskStatus(taskId, "COMPLETED", totalProcessed, totalProcessed, null);
            logger.info("导入任务完成，taskId: {}, 总处理记录数: {}", taskId, totalProcessed);
            
        } catch (Exception e) {
            logger.error("导入任务失败，taskId: {}", taskId, e);
            taskStatusService.updateTaskStatus(taskId, "FAILED", null, null, e.getMessage());
        }
        
        return CompletableFuture.completedFuture(null);
    }

    /**
     * 异步导出数据，支持大数据集分割
     */
    @Async("taskExecutor")
    public CompletableFuture<String> exportData(String taskId, ExportRequest request) {
        logger.info("开始异步导出任务，taskId: {}, 时间范围: {} - {}", 
                   taskId, request.getStartTime(), request.getEndTime());
        
        try {
            taskStatusService.updateTaskStatus(taskId, "RUNNING", null, null, null);
            
            // 计算总记录数
            long totalRecords = influxDBService.countByTimeRange(request.getStartTime(), request.getEndTime());
            taskStatusService.updateTaskStatus(taskId, "RUNNING", 0L, totalRecords, null);
            
            if (totalRecords == 0) {
                taskStatusService.updateTaskStatus(taskId, "COMPLETED", 0L, 0L, "没有找到指定时间范围内的数据");
                return CompletableFuture.completedFuture("no_data");
            }

            // 如果数据量小于限制，直接导出单个文件
            if (totalRecords <= request.getMaxRecordsPerFile()) {
                String filePath = exportSingleFile(taskId, request);
                taskStatusService.updateTaskStatus(taskId, "COMPLETED", totalRecords, totalRecords, null);
                return CompletableFuture.completedFuture(filePath);
            }

            // 大数据集按时间分割导出
            List<String> filePaths = exportMultipleFiles(taskId, request, totalRecords);
            
            // 创建ZIP文件
            String zipPath = createZipFile(taskId, filePaths);
            
            // 清理临时文件
            cleanupTempFiles(filePaths);
            
            taskStatusService.updateTaskStatus(taskId, "COMPLETED", totalRecords, totalRecords, null);
            logger.info("导出任务完成，taskId: {}, ZIP文件: {}", taskId, zipPath);
            
            return CompletableFuture.completedFuture(zipPath);
            
        } catch (Exception e) {
            logger.error("导出任务失败，taskId: {}", taskId, e);
            taskStatusService.updateTaskStatus(taskId, "FAILED", null, null, e.getMessage());
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * 导出单个文件
     */
    private String exportSingleFile(String taskId, ExportRequest request) throws IOException {
        List<TimeSeriesData> data = influxDBService.queryByTimeRange(request.getStartTime(), request.getEndTime());
        
        String fileName = String.format("export_%s.csv", taskId);
        Path filePath = Paths.get(System.getProperty("java.io.tmpdir"), fileName);
        
        try (FileOutputStream fos = new FileOutputStream(filePath.toFile())) {
            csvProcessingService.writeCSV(data, fos);
        }
        
        return filePath.toString();
    }

    /**
     * 按时间分割导出多个文件
     */
    private List<String> exportMultipleFiles(String taskId, ExportRequest request, long totalRecords) throws IOException {
        List<String> filePaths = new ArrayList<>();
        
        // 计算需要分割的时间段数
        long estimatedFiles = (totalRecords / request.getMaxRecordsPerFile()) + 1;
        Duration totalDuration = Duration.between(request.getStartTime(), request.getEndTime());
        Duration segmentDuration = totalDuration.dividedBy(estimatedFiles);
        
        Instant currentStart = request.getStartTime();
        int fileIndex = 1;
        long processedRecords = 0;
        
        while (currentStart.isBefore(request.getEndTime())) {
            Instant currentEnd = currentStart.plus(segmentDuration);
            if (currentEnd.isAfter(request.getEndTime())) {
                currentEnd = request.getEndTime();
            }
            
            List<TimeSeriesData> segmentData = influxDBService.queryByTimeRange(currentStart, currentEnd);
            
            if (!segmentData.isEmpty()) {
                String fileName = String.format("export_%s_part_%03d.csv", taskId, fileIndex);
                Path filePath = Paths.get(System.getProperty("java.io.tmpdir"), fileName);
                
                try (FileOutputStream fos = new FileOutputStream(filePath.toFile())) {
                    csvProcessingService.writeCSV(segmentData, fos);
                }
                
                filePaths.add(filePath.toString());
                processedRecords += segmentData.size();
                
                // 更新进度
                taskStatusService.updateTaskStatus(taskId, "RUNNING", processedRecords, totalRecords, null);
                
                logger.info("导出文件 {} 完成，包含 {} 条记录", fileName, segmentData.size());
                fileIndex++;
            }
            
            currentStart = currentEnd;
        }
        
        return filePaths;
    }

    /**
     * 创建ZIP文件
     */
    private String createZipFile(String taskId, List<String> filePaths) throws IOException {
        String zipFileName = String.format("export_%s.zip", taskId);
        Path zipPath = Paths.get(System.getProperty("java.io.tmpdir"), zipFileName);
        
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipPath.toFile()))) {
            for (String filePath : filePaths) {
                File file = new File(filePath);
                ZipEntry zipEntry = new ZipEntry(file.getName());
                zos.putNextEntry(zipEntry);
                
                try (FileInputStream fis = new FileInputStream(file)) {
                    byte[] buffer = new byte[8192];
                    int length;
                    while ((length = fis.read(buffer)) > 0) {
                        zos.write(buffer, 0, length);
                    }
                }
                zos.closeEntry();
            }
        }
        
        return zipPath.toString();
    }

    /**
     * 清理临时文件
     */
    private void cleanupTempFiles(List<String> filePaths) {
        for (String filePath : filePaths) {
            try {
                Files.deleteIfExists(Paths.get(filePath));
            } catch (IOException e) {
                logger.warn("清理临时文件失败: {}", filePath, e);
            }
        }
    }
}

// 11. REST控制器
package com.example.influxdb.controller;

import com.example.influxdb.dto.ExportRequest;
import com.example.influxdb.entity.TaskStatus;
import com.example.influxdb.service.AsyncImportExportService;
import com.example.influxdb.service.TaskStatusService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.Valid;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/influxdb")
public class InfluxDBController {

    @Autowired
    private AsyncImportExportService asyncImportExportService;
    
    @Autowired
    private TaskStatusService taskStatusService;

    /**
     * 异步导入CSV文件
     */
    @PostMapping("/import")
    public ResponseEntity<Map<String, Object>> importData(@RequestParam("file") MultipartFile file) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            // 验证文件
            if (file.isEmpty() || !file.getOriginalFilename().toLowerCase().endsWith(".csv")) {
                response.put("success", false);
                response.put("message", "请上传有效的CSV文件");
                return ResponseEntity.badRequest().body(response);
            }
            
            // 创建任务
            String taskId = taskStatusService.createTask("IMPORT");
            
            // 异步处理
            asyncImportExportService.importLargeFile(taskId, file);
            
            response.put("success", true);
            response.put("taskId", taskId);
            response.put("message", "导入任务已开始，请使用taskId查询进度");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "导入失败: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * 异步导出数据
     */
    @PostMapping("/export")
    public ResponseEntity<Map<String, Object>> exportData(@Valid @RequestBody ExportRequest request) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            // 验证时间范围
            if (request.getStartTime().isAfter(request.getEndTime())) {
                response.put("success", false);
                response.put("message", "开始时间不能晚于结束时间");
                return ResponseEntity.badRequest().body(response);
            }
            
            // 创建任务
            String taskId = taskStatusService.createTask("EXPORT");
            
            // 异步处理
            asyncImportExportService.exportData(taskId, request);
            
            response.put("success", true);
            response.put("taskId", taskId);
            response.put("message", "导出任务已开始，请使用taskId查询进度");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "导出失败: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    /**
     * 查询任务状态
     */
    @GetMapping("/task/{taskId}/status")
    public ResponseEntity<Map<String, Object>> getTaskStatus(@PathVariable String taskId) {
        Map<String, Object> response = new HashMap<>();
        
        Optional<TaskStatus> taskStatus = taskStatusService.getTaskStatus(taskId);
        
        if (taskStatus.isPresent()) {
            TaskStatus task = taskStatus.get();
            response.put("success", true);
            response.put("taskId", task.getTaskId());
            response.put("taskType", task.getTaskType());
            response.put("status", task.getStatus());
            response.put("progress", task.getProgress());
            response.put("processedRecords", task.getProcessedRecords());
            response.put("totalRecords", task.getTotalRecords());
            response.put("errorMessage", task.getErrorMessage());
            response.put("createdTime", task.getCreatedTime());
            response.put("updatedTime", task.getUpdatedTime());
            
            return ResponseEntity.ok(response);
        } else {
            response.put("success", false);
            response.put("message", "任务不存在");
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * 下载导出文件
     */
    @GetMapping("/task/{taskId}/download")
    public ResponseEntity<Resource> downloadFile(@PathVariable String taskId) {
        Optional<TaskStatus> taskStatus = taskStatusService.getTaskStatus(taskId);
        
        if (!taskStatus.isPresent()) {
            return ResponseEntity.notFound().build();
        }
        
        TaskStatus task = taskStatus.get();
        
        // 检查任务状态
        if (!"COMPLETED".equals(task.getStatus()) || !"EXPORT".equals(task.getTaskType())) {
            return ResponseEntity.badRequest().build();
        }
        
        try {
            // 构造文件路径
            String fileName;
            if (task.getTotalRecords() <= 100000) {
                fileName = String.format("export_%s.csv", taskId);
            } else {
                fileName = String.format("export_%s.zip", taskId);
            }
            
            String filePath = System.getProperty("java.io.tmpdir") + File.separator + fileName;
            File file = new File(filePath);
            
            if (!file.exists()) {
                return ResponseEntity.notFound().build();
            }
            
            Resource resource = new FileSystemResource(file);
            
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(resource);
                    
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * 取消任务
     */
    @PostMapping("/task/{taskId}/cancel")
    public ResponseEntity<Map<String, Object>> cancelTask(@PathVariable String taskId) {
        Map<String, Object> response = new HashMap<>();
        
        Optional<TaskStatus> taskStatus = taskStatusService.getTaskStatus(taskId);
        
        if (taskStatus.isPresent()) {
            TaskStatus task = taskStatus.get();
            
            if ("RUNNING".equals(task.getStatus())) {
                taskStatusService.updateTaskStatus(taskId, "CANCELLED", null, null, "用户取消任务");
                response.put("success", true);
                response.put("message", "任务已取消");
            } else {
                response.put("success", false);
                response.put("message", "只能取消运行中的任务");
            }
            
            return ResponseEntity.ok(response);
        } else {
            response.put("success", false);
            response.put("message", "任务不存在");
            return ResponseEntity.notFound().build();
        }
    }
}

// 12. 配置文件 (application.yml)
/*
spring:
  application:
    name: influxdb-import-export
  datasource:
    url: jdbc:h2:mem:testdb
    driverClassName: org.h2.Driver
    username: sa
    password: 
  jpa:
    database-platform: org.hibernate.dialect.H2Dialect
    hibernate:
      ddl-auto: create-drop
    show-sql: true
  servlet:
    multipart:
      max-file-size: 500MB
      max-request-size: 500MB

influxdb:
  url: http://localhost:8086
  token: your-influxdb-token
  org: your-org
  bucket: your-bucket
  measurement: sensor_data

logging:
  level:
    com.example.influxdb: DEBUG
    org.springframework.web.multipart: DEBUG

server:
  port: 8080
*/

// 13. 全局异常处理器
package com.example.influxdb.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

import java.util.HashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<Map<String, Object>> handleMaxUploadSizeExceeded(MaxUploadSizeExceededException e) {
        logger.error("文件大小超过限制", e);
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("message", "文件大小超过限制（最大500MB）");
        
        return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGenericException(Exception e) {
        logger.error("系统异常", e);
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", false);
        response.put("message", "系统内部错误");
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
}

// 14. 使用示例和测试
/*
使用示例：

1. 导入CSV文件：
curl -X POST -F "file=@data.csv" http://localhost:8080/api/influxdb/import

2. 导出数据：
curl -X POST -H "Content-Type: application/json" \
  -d '{"startTime":"2025-07-23T00:00:00Z","endTime":"2025-07-23T23:59:59Z","maxRecordsPerFile":50000}' \
  http://localhost:8080/api/influxdb/export

3. 查询任务状态：
curl http://localhost:8080/api/influxdb/task/{taskId}/status

4. 下载导出文件：
curl -O http://localhost:8080/api/influxdb/task/{taskId}/download

5. 取消任务：
curl -X POST http://localhost:8080/api/influxdb/task/{taskId}/cancel

CSV文件格式示例：
time,tag1,tag2,tag3
2025-07-23 10:21:33,10,20,30
2025-07-23 10:21:34,11,21,31
2025-07-23 10:21:35,12,22,32

特性说明：
1. 支持大文件流式读取，避免内存溢出
2. 异步处理，不阻塞请求
3. 实时任务进度跟踪
4. 大数据集自动按时间分片导出
5. 自动压缩多文件为ZIP
6. 错误处理和任务状态管理
7. 支持任务取消
8. 文件大小和格式验证
*/