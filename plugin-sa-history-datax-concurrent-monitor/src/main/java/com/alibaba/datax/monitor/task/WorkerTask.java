package com.alibaba.datax.monitor.task;

import cn.hutool.core.collection.LineIter;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class WorkerTask implements Runnable, Serializable {

    private static final long serialVersionUID = 7336030667856391577L;

    private static String OS = System.getProperty("os.name").toLowerCase();

    private String command;
    private CountDownLatch countDownLatch;

    public WorkerTask(String command,CountDownLatch countDownLatch) {
        this.command = command;
        this.countDownLatch = countDownLatch;
    }

    public static boolean isWindows(){
        return OS.indexOf("windows")>=0;
    }

    @Override
    public void run() {
        if(StrUtil.isBlank(this.command)){
            return;
        }
        InputStream errorStream = null,inputStream = null;
        Runtime runtime = Runtime.getRuntime();
        try {
            log.info("开始执行任务:{}",this.command);
            String[] cmd = { "sh", "-c", this.command };
            Process exec = null;
            if (isWindows()) {
                exec = runtime.exec(this.command);
            }else{
                exec = runtime.exec(cmd);
            }
            errorStream = exec.getErrorStream();
            inputStream = exec.getInputStream();
            LineIter inputStreamLineIter = IoUtil.lineIter(inputStream, Charset.defaultCharset());
            LineIter errorStreamLineIter = IoUtil.lineIter(errorStream, Charset.defaultCharset());
            while (inputStreamLineIter.hasNext() || errorStreamLineIter.hasNext()) {
                if(inputStreamLineIter.hasNext()){
                    log.info(inputStreamLineIter.next());
                }
                if(errorStreamLineIter.hasNext()){
                    log.error(errorStreamLineIter.next());
                }
            }
            int status = exec.waitFor();
            log.info("任务运行状态:{}",status);
            if(0 != status){
                //非正常退出
            }
        } catch (IOException | InterruptedException e) {
            //执行出错
            e.printStackTrace();
            log.error("执行出错",e);
        }finally {
            this.countDownLatch.countDown();
            if(errorStream != null){
                try {
                    errorStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(inputStream != null){
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
