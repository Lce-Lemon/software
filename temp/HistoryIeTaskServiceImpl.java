package com.hollysys.sync.service.impl;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hollysys.smartfactory.common.exception.CustomException;
import com.hollysys.smartfactory.common.util.StringUtils;
import com.hollysys.sync.constant.HistoryIeTaskConstant;
import com.hollysys.sync.mapper.HistoryIeTaskMapper;
import com.hollysys.sync.model.entity.HistoryIeTask;
import com.hollysys.sync.model.req.HistoryIeTaskPageKeywordReq;
import com.hollysys.sync.model.req.HistoryIeTaskSaveReq;
import com.hollysys.sync.model.req.HistoryIeTaskUpdateReq;
import com.hollysys.sync.model.resp.HistoryIeTaskResp;
import com.hollysys.sync.service.HistoryIeTaskService;
import com.hollysys.sync.service.HistoryInfluxDbService;
import com.hollysys.sync.util.Assert;
import com.hollysys.sync.util.ConvertUtils;
import com.opencsv.CSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * 历史数据导入导出任务实现
 */
@Service
@Slf4j
public class HistoryIeTaskServiceImpl extends ServiceImpl<HistoryIeTaskMapper, HistoryIeTask> implements HistoryIeTaskService {

    private final Map<String, Future<?>> runningTasks = new ConcurrentHashMap<>();

    @Resource(name = "historyTaskExecutor")
    private ThreadPoolTaskExecutor historyTaskExecutor;

    @Resource
    private HistoryIeTaskMapper mapper;

    @Resource
    private HistoryInfluxDbService dbService;

    @Resource
    private CsvProcessingService csvProcessingService;

    /**
     * 默认分隔时间：5分钟
     */
    @Value("${history.ie.task.defaultSplitTime:5}")
    private Long defaultSplitTime;

    @Value("${history.ie.task.maxRecordsPerFile:100000}")
    private Long maxRecordsPerFile;

    @Value("${history.ie.task.exportPath}")
    private String exportPath;

    @Resource(name = "influxQueryExecutor")
    private ThreadPoolTaskExecutor influxQueryExecutor;

    @Value("${history.ie.task.maxFileSize:10485760}")
    private Long maxFileSize;

    @Override
    public IPage<HistoryIeTaskResp> findAll(HistoryIeTaskPageKeywordReq req) {
        Executors.newFixedThreadPool(3);

        IPage<HistoryIeTaskResp> iPage = Page.of(req.getPage(), req.getPageSize());
        List<HistoryIeTask> list = mapper.findAll(iPage, req);
        iPage.setRecords(ConvertUtils.convertList(list, HistoryIeTaskResp.class, (t, s) -> {
            if (Objects.nonNull(s.getFileType())) {
                t.setFileType(s.getFileType().toString());
            }
            t.setDbunitList(Arrays.stream(StringUtils.split(s.getDbunitList(), ",")).collect(Collectors.toList()))
                    .setTaskType(s.getTaskType().toString());
        }));
        return iPage;
    }

    @Override
    public boolean insert(HistoryIeTaskSaveReq req) {
        // 保存任务信息
        HistoryIeTask importTask = ConvertUtils.convert(req, HistoryIeTask.class)
                .setTaskType(Integer.parseInt(req.getTaskType()))
                .setStatus(HistoryIeTaskConstant.StatusEnum.NOT_EXECUTED.key())
                .setDbunitList(String.join(",", req.getDbunitList()))
                .setPkid(StringUtils.uuid(true))
                .setCreateTime(Instant.now())
                .setUpdateTime(Instant.now());
        if (StringUtils.isNotBlank(req.getFileType())) {
            importTask.setFileType(Integer.parseInt(req.getFileType()));
        }
        return save(importTask);
    }

    @Override
    public boolean edit(HistoryIeTaskUpdateReq req) {
        // 判断任务信息是否存在
        validateById(req.getPkid());

        // 更新任务信息
        HistoryIeTask updateModel = ConvertUtils.convert(req, HistoryIeTask.class, (t, s) -> {
            t.setTaskType(null);
            t.setUpdateTime(Instant.now());
        });

        return updateById(updateModel);
    }

    @Override
    public boolean delete(String pkid) {
        // 校验并获取任务信息
        validateById(pkid);
        return removeById(pkid);
    }

    @Override
    public boolean execute(String pkid) {
        HistoryIeTask historyIeTask = validateById(pkid);
        boolean update = updateStatus(HistoryIeTaskConstant.StatusEnum.PENDING, pkid);

        if (update) {
            submitTask(pkid, () -> asyncTask(historyIeTask), () -> {
                updateStatus(HistoryIeTaskConstant.StatusEnum.FAILED, pkid);
            });
        }

        return update;
    }

    /**
     * 更新状态
     *
     * @param statusEnum 状态枚举
     * @param pkid       主键
     * @return boolean
     * @author Leiquan.Guo
     * @date 2025/7/25 9:38
     * @description 更新状态
     */
    private boolean updateStatus(HistoryIeTaskConstant.StatusEnum statusEnum, String pkid) {
        return lambdaUpdate().set(HistoryIeTask::getStatus, statusEnum.key())
                .set(HistoryIeTaskConstant.StatusEnum.IN_PROGRESS.equals(statusEnum), HistoryIeTask::getExecuteStartTime, Instant.now())
                .set(HistoryIeTaskConstant.StatusEnum.FAILED.equals(statusEnum) ||
                        HistoryIeTaskConstant.StatusEnum.COMPLETED.equals(statusEnum), HistoryIeTask::getExecuteEndTime, Instant.now())
                .set(HistoryIeTask::getUpdateTime, Instant.now())
                .eq(HistoryIeTask::getPkid, pkid).update();
    }

    private void asyncTask(HistoryIeTask historyIeTask) {
        String pkid = historyIeTask.getPkid();
        // 更新为执行中
        boolean update = updateStatus(HistoryIeTaskConstant.StatusEnum.IN_PROGRESS, pkid);
        if (!update) {
            // 更新失败
            return;
        }

        // 导入任务
        if (HistoryIeTaskConstant.TaskTypeEnum.IMPORT.key().equals(historyIeTask.getTaskType())) {
            importTask(historyIeTask);
        } else {
            exportTask(historyIeTask);
        }

        updateStatus(HistoryIeTaskConstant.StatusEnum.COMPLETED, pkid);
    }

    private void exportTask(HistoryIeTask historyIeTask) {
        String taskId = historyIeTask.getPkid();
        log.info("开始异步导出任务，taskId: {}, dbunitList: {}, 时间范围: {} - {}",
                taskId, historyIeTask.getDbunitList(), historyIeTask.getStartTime(), historyIeTask.getEndTime());

        // 数据库单元
        String[] split = historyIeTask.getDbunitList().split(",");
        // 时间范围
        List<Instant[]> times = getTimes(historyIeTask.getStartTime(), historyIeTask.getEndTime());

        // csv注释信息

        for (String dbunit : split) {
            String[] firstStr = new String[]{"#config", historyIeTask.getName(), dbunit,
                    historyIeTask.getStartTime().toString(), historyIeTask.getEndTime().toString()};

            String[] csvHeader = getCsvHeader(historyIeTask.getExportFileStyle(), dbunit, times);

            AtomicInteger fileIndex = new AtomicInteger(1);
            AtomicLong totalSize = new AtomicLong(0L);
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            Set<String> fileNames = Sets.newConcurrentHashSet();

            for (Instant[] timeRange : times) {
                // 多线程查询历史库时间范围内数据并写入文件
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        // Query data for the given time range
                        List<HistoryInfluxDbServiceImpl.IeCsvModel> data = dbService.queryByTimeRange(dbunit, timeRange[0], timeRange[1]);

                        // Write data to files with a size limit of 100MB
                        writeDataToFilesWithSizeLimit(historyIeTask, firstStr, csvHeader, data, dbunit, fileIndex, totalSize, fileNames);
                    } catch (Exception e) {
                        log.error("Error querying or writing data for time range: {} - {}, dbunit: {}", timeRange[0], timeRange[1], dbunit);
                        log.error(e.getMessage(), e);
                    }
                }, influxQueryExecutor);
                futures.add(future);
            }

            // Wait for all threads to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            try {
                createZipFile(dbunit, fileNames);
            } catch (IOException e) {
                throw new CustomException(e);
            }
        }
    }

    private void writeDataToFilesWithSizeLimit(HistoryIeTask historyIeTask, String[] csvConfig, String[] csvHeader,
                                               List<HistoryInfluxDbServiceImpl.IeCsvModel> dataList,
                                               String dbunit, AtomicInteger fileIndex, AtomicLong totalSize, Set<String> fileNames) throws IOException {
        int fileIndexItem = fileIndex.get();
        long totalSizeItem = totalSize.get();

        String fileName = String.format("%s/export_%s_part_%03d.csv", exportPath, dbunit, fileIndexItem);
        fileNames.add(fileName);

        try (OutputStreamWriter writer = new OutputStreamWriter(Files.newOutputStream(Paths.get(fileName), StandardOpenOption.CREATE, StandardOpenOption.APPEND));
             CSVWriter csvWriter = new CSVWriter(writer)) {

            // 文件大小为0，写入头部
            if (totalSizeItem == 0L) {
                // Write header
                csvWriter.writeNext(csvConfig);
                totalSize.addAndGet(rowToBytes(csvConfig).length);
                csvWriter.writeNext(csvHeader);
                totalSize.addAndGet(rowToBytes(csvHeader).length);
            }

            // Write data rows
            for (HistoryInfluxDbServiceImpl.IeCsvModel data : dataList) {

                String[] row = new String[csvHeader.length];
                row[0] = data.getTime().toString();

                // 行模式
                if (HistoryIeTaskConstant.ExportFileStyleEnum.ROW.key().equals(historyIeTask.getExportFileStyle())) {
                    for (Map.Entry<String, Object> entry: data.getOtherColumns().entrySet()) {
                        row[1] = entry.getKey();
                        row[2] = entry.getValue().toString();

                        // 写入并增加大小
                        csvWriter.writeNext(row);
                        totalSize.addAndGet(rowToBytes(row).length);
                    }
                } else {
                    for (int i = 1; i < csvHeader.length; i++) {
                        String field = csvHeader[i];
                        Object value = null;

                        if (data.getOtherColumns() != null && data.getOtherColumns().containsKey(field)) {
                            value = data.getOtherColumns().get(field);
                        }

                        row[i] = value != null ? value.toString() : "";
                    }

                    csvWriter.writeNext(row);
                    totalSize.addAndGet(rowToBytes(row).length);
                }

                if (totalSize.get() >= maxFileSize) {
                    csvWriter.flush();

                    // 自增文件名称并重置文件大小
                    fileIndex.incrementAndGet();
                    totalSize.set(0L);
                }
            }
        }
    }

    private byte[] rowToBytes(String[] row) {
        return String.join(",", row).getBytes(StandardCharsets.UTF_8);
    }

    private String[] getCsvHeader(Integer exportFileStyle, String dbunit, List<Instant[]> times) {
        if (HistoryIeTaskConstant.ExportFileStyleEnum.ROW.key().equals(exportFileStyle)) {
            return new String[]{"time", "nodeId", "value"};
        }
        Set<String> uniqueNodeIds = Sets.newHashSet();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Instant[] item : times) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    List<String> nodeIds = dbService.queryNodeIds(dbunit, item[0], item[1]);
                    uniqueNodeIds.addAll(nodeIds);
                } catch (Exception e) {
                    log.error("查询节点ID失败, dbunit: {}", dbunit, e);
                }
            }, influxQueryExecutor);
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        return Stream.concat(Stream.of("time"), uniqueNodeIds.stream()).toArray(String[]::new);
    }

    private List<Instant[]> getTimes(Instant startTime, Instant endTime) {
        Duration splitTime = Duration.ofMinutes(defaultSplitTime);
        List<Instant[]> times = Lists.newArrayList();
        while (true) {
            Instant[] item = new Instant[2];
            item[0] = startTime;
            startTime = startTime.plus(splitTime);
            if (startTime.isAfter(endTime) || startTime.equals(endTime)) {
                item[1] = endTime;
                times.add(item);
                break;
            }
            item[1] = startTime;
            times.add(item);
        }

        return times;
    }

    private void importTask(HistoryIeTask historyIeTask) {

    }

    public HistoryIeTask validateById(String pkid) {
        HistoryIeTask importTask = getById(pkid);
        Assert.notNull(importTask, () -> "任务信息不存在");
        return importTask;
    }

    public String submitTask(String taskId, Runnable task, Runnable exceptionRunnable) {
        return submitTask(taskId, task, exceptionRunnable, null);
    }

    public String submitTask(String taskId, Runnable task, Runnable exceptionRunnable, Runnable completeRunnable) {
        int queueSize = historyTaskExecutor.getQueueSize();
        int queueCapacity = historyTaskExecutor.getQueueCapacity();
        if (queueSize + 1 > queueCapacity) {
            throw new CustomException("任务队列已超限(" + queueCapacity + ")，请稍后再试!");
        }
        Future<?> future = historyTaskExecutor.submit(() -> {
            try {
                task.run();
                log.info("任务执行成功, taskId: {}", taskId);
            } catch (Exception e) {
                exceptionRunnable.run();
                log.error("任务执行失败, taskId: " + taskId, e);
            } finally {
                runningTasks.remove(taskId);
                if (Objects.nonNull(completeRunnable)) {
                    completeRunnable.run();
                }
            }
        });
        runningTasks.put(taskId, future);
        return taskId;
    }

    public boolean cancelTask(String taskId) {
        Future<?> future = runningTasks.remove(taskId);
        if (future != null) {
            return future.cancel(true);
        }
        return false;
    }

    /**
     * 导出单个文件
     */
    private String exportSingleFile(HistoryIeTask historyIeTask, String dbunit) throws IOException {
        List<HistoryInfluxDbServiceImpl.IeCsvModel> data = dbService.queryByTimeRange(dbunit, historyIeTask.getStartTime(), historyIeTask.getEndTime());

        String fileName = String.format("%s/export_%s.csv", exportPath, dbunit);
        try (FileOutputStream fos = new FileOutputStream(fileName)) {
            csvProcessingService.writeCSV(data, fos);
        }

        return fileName;
    }

    /**
     * 按时间分割导出多个文件
     */
    private List<String> exportMultipleFiles(HistoryIeTask historyIeTask, String dbunit, long totalRecords) throws IOException {
        List<String> filePaths = new ArrayList<>();

        // 计算需要分割的时间段数
        long estimatedFiles = (totalRecords / maxRecordsPerFile) + 1;
        Duration totalDuration = Duration.between(historyIeTask.getStartTime(), historyIeTask.getEndTime());
        Duration segmentDuration = totalDuration.dividedBy(estimatedFiles);

        Instant currentStart = historyIeTask.getStartTime();
        int fileIndex = 1;
        long processedRecords = 0;

        while (currentStart.isBefore(historyIeTask.getEndTime())) {
            Instant currentEnd = currentStart.plus(segmentDuration);
            if (currentEnd.isAfter(historyIeTask.getEndTime())) {
                currentEnd = historyIeTask.getEndTime();
            }

            List<HistoryInfluxDbServiceImpl.IeCsvModel> segmentData = dbService.queryByTimeRange(dbunit, currentStart, currentEnd);

            if (!segmentData.isEmpty()) {
                String fileName = String.format("%s/export_%s_part_%03d.csv", exportPath, dbunit, fileIndex);
                try (FileOutputStream fos = new FileOutputStream(fileName)) {
                    csvProcessingService.writeCSV(segmentData, fos);
                }

                filePaths.add(fileName);
                processedRecords += segmentData.size();

                // 更新进度

                fileIndex++;
            }

            currentStart = currentEnd;
        }

        return filePaths;
    }

    /**
     * 创建ZIP文件
     */
    private String createZipFile(String dbunit, Set<String> filePaths) throws IOException {
        String zipFileName = String.format("%s/export_%s.zip", exportPath, dbunit);

        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(Paths.get(zipFileName)))) {
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

        return zipFileName;
    }

    /**
     * 清理临时文件
     */
    private void cleanupTempFiles(List<String> filePaths) {
        for (String filePath : filePaths) {
            try {
                Files.deleteIfExists(Paths.get(filePath));
            } catch (IOException e) {
                log.warn("清理临时文件失败: {}", filePath, e);
            }
        }
    }
}
