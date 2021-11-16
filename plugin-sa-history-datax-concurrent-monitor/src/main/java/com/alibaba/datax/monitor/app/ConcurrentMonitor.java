package com.alibaba.datax.monitor.app;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.datax.monitor.service.ThreadPoolService;
import com.alibaba.datax.monitor.task.WorkerTask;
import org.apache.commons.cli.*;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ConcurrentMonitor {

    public static CommandLine cmd;

    public static Options initOptions() {

        Options options = new Options();

        options.addOption("h", "help", false, "显示帮助文档");
        options.addOption(
                Option.builder("c")
                        .longOpt("channel")
                        .hasArg()
                        .desc("并发数，同时运行的任务数，默认值为：如任务数大于执行物理机cpu核数,则为cpu核数，否则为任务数(-m指定的数量加上-f中指定的数量)")
                        .build()
        );
        options.addOption(
                Option.builder("p")
                        .longOpt("pool")
                        .hasArg()
                        .desc("最大队列数,默认值500")
                        .build()
        );
        options.addOption(
                Option.builder("m")
                        .longOpt("cmd")
                        .hasArg()
                        .desc("执行的命令集")
                        .build()
        );
        options.addOption(
                Option.builder("f")
                        .longOpt("file")
                        .hasArg()
                        .desc("命令集文件，文件中一行为一个任务")
                        .build()
        );
        return options;
    }

    public static void main(String[] args) {
        Options options = initOptions();
        if (args.length == 0) {
            help(options);
            return;
        }
        CommandLineParser parser = new DefaultParser();
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            help(options);
            return;
        }
        if (cmd.hasOption("h")) {
            help(options);
            return;
        }
        help(options);
        List<String> cmds = new ArrayList<>();
        String[] commands = cmd.getOptionValues('m');
        String[] fss = cmd.getOptionValues('f');
        if(!(Objects.isNull(fss) || fss.length == 0)){
            for (String fs : fss) {
                if(StrUtil.isNotBlank(fs)){
                    File file = new File(fs);
                    if(file.isFile()){
                        List<String> cmdList = FileUtil.readLines(file, Charset.defaultCharset());
                        if(!(Objects.isNull(cmdList) || cmdList.isEmpty())){
                            cmds.addAll(cmdList);
                        }
                    }
                }
            }
        }
        if(!(Objects.isNull(commands) || commands.length == 0)){
            for (int i = 0; i < commands.length; i++) {
                cmds.add(commands[i]);
            }
        }
        if (cmds.isEmpty()) {
            help(options);
            return;
        }
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        String concurrentNum = cmd.getOptionValue('c', (cmds.size() > availableProcessors?availableProcessors:cmds.size())  + "");
        String maxPoolSize = cmd.getOptionValue('p', "500");
        ThreadPoolService threadPoolService = new ThreadPoolService(Integer.parseInt(concurrentNum), Integer.parseInt(maxPoolSize),cmds.size());
        for (String command : cmds) {
            threadPoolService.submit(new WorkerTask(command,threadPoolService.getCountDownLatch()));
        }
        threadPoolService.shutdown();

    }

    public static void help(Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        // 这里显示简短的帮助信息
        hf.printHelp("java -jar <*.jar>", options, true);
    }

}
