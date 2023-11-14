package com.alibaba.datax.plugin.util;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class Timer {

  /**
   * 多线程下关联不同的Timer的耗时统计。多线程下不同的Timer通过该值关联起来
   */
  private String traceId = UUID.randomUUID().toString();
  private long timeoutMillisecond;
  private long startTimestamp;
  private Long endTimestamp;

  private List<Long> stepTimeConsumeList = new Vector<>(64);

  private List<Long> lastStepTimeConsumeList = new Vector<>(64);

  private List<String> curStepStackTraceList = new Vector<>(64);

  private List<String> stepTimeConsumeDescList = new Vector<>(64);

  private Map<String,Timer> childMap = new ConcurrentHashMap<>(16);

  private List<String> childNameList = new Vector<>(16);

  /**
   * 智能展示时间时，最大展示的单位
   *    eg:若不限制时展示为：2 d 20 m 30 s
   *    若设置为 Level.DAY          则展示为：2 d 20 m 30 s
   *    若设置为 Level.HOUR         则展示为：20 m 30 s
   *    若设置为 Level.MINUTE       则展示为：20 m 30 s
   *    若设置为 Level.SECOND       则展示为：30 s
   *    若设置为 Level.MILLISECOND  则展示为：174030000 ms
   */
  private Level level = Level.DAY;

  /**
   * 智能展示时间时，最多展示多少层级（最多展示多少个单位），小于等于零时则不限制。
   *    eg:若不限制时展示为：2 d 20 m 30 s
   *    若设置为 1 则展示为：2 d
   *    若设置为 2 则展示为：2 d 20 m
   *    若设置为 3 则展示为：2 d 20 m 30 s
   */
  private int levelMaxCount = 0;

  public static void main(String[] args) throws InterruptedException {
    Timer timer = new Timer(UUID.randomUUID().toString().replaceAll("-", ""));
//    Timer timer = new Timer();
    Thread.sleep(1000);
    timer.stepElapsedMillisecond("aa123456789123456789009876543212ss");
    System.out.println("step 1 cost(ms):"+timer.elapsedMillisecond());
    Thread.sleep(2*1000);
    timer.stepElapsedMillisecond("bb");
    timer.stepElapsedMillisecond();
    Thread.sleep(3*1000);
    timer.stepElapsedMillisecond("cc");
    timer.stepElapsedMillisecond();

//    timer.printAllStepIntelligentShowUnit();

    LocalDateTime now = LocalDateTime.now();
    LocalDateTime localDateTime = now.minusDays(2).minusMinutes(20).minusSeconds(30);
    long i = now.toInstant(ZoneOffset.of("+8")).toEpochMilli() - localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
//    System.out.println(timer.format(i));
//    System.out.println(timer.getAllStepIntelligentShowUnit());
//    timer.reset();
    System.out.println("=============================");
//    System.out.println(timer.getAllStepIntelligentShowUnit());

    ExecutorService executorService = Executors.newCachedThreadPool();
    Timer syncTimer = timer.newChildTimerAndHold("syncTimer");
    Timer syncTimer2 = syncTimer.newChildTimerAndHold("syncTimer2");
    executorService.submit(new Runnable() {
      @SneakyThrows
      @Override
      public void run() {
//        String traceId1 = timer.getTraceId();
//        Timer syncTimer = new Timer(traceId1);

        Thread.sleep(1000);
        syncTimer.stepElapsedMillisecond("syncTimer.aa123456789123456789009876543212ss");
        Thread.sleep(2*1000);
        syncTimer.stepElapsedMillisecond("syncTimer.bb");
        syncTimer.stepElapsedMillisecond();
        Thread.sleep(3*1000);
        syncTimer.stepElapsedMillisecond("syncTimer.cc");
        syncTimer.stepElapsedMillisecond();

//        syncTimer.printAllStepIntelligentShowUnit();
      }
    });
    executorService.submit(new Runnable() {
      @SneakyThrows
      @Override
      public void run() {
//        String traceId1 = timer.getTraceId();
//        Timer syncTimer2 = new Timer(traceId1);
//        Timer syncTimer2 = timer.newChildTimerAndHold("syncTimer2");
//        Timer syncTimer2 = timer.newChildTimerAndHold("syncTimer2");
        Thread.sleep(1000);
        syncTimer2.stepElapsedMillisecond("syncTimer2.aa123456789123456789009876543212ss");
        Thread.sleep(2*1000);
        syncTimer2.stepElapsedMillisecond("syncTimer2.bb");
        syncTimer2.stepElapsedMillisecond();
        Thread.sleep(3*1000);
        syncTimer2.stepElapsedMillisecond("syncTimer2.cc");
        syncTimer2.stepElapsedMillisecond();

//        syncTimer.printAllStepIntelligentShowUnit();
      }
    });
    executorService.shutdown();
    Timer.sleepSec(8);
    System.out.println("========1111111111111111111111111111=====================");
    timer.printAllStepIntelligentShowUnit(true,true);

    System.out.println("========2222222222222222222222222222=====================");
    System.out.println(timer.getAllStepIntelligentShowUnit(true,true));

    System.out.println("========3333333333333333333333333333=====================");
//    Timer syncTimer02 = timer.getChild("syncTimer2");
    Timer syncTimer02 = syncTimer.getChild("syncTimer2");
    if(syncTimer02 != null){
      syncTimer02.printAllStepIntelligentShowUnit();
    }

  }

  public static Timer newTimer() {
    return new Timer();
  }

  public static Timer newTimer(String traceId) {
    return new Timer(traceId);
  }

  public static Timer newTimer(long timeoutSecond) {
    return new Timer(timeoutSecond);
  }

  public static Timer newTimer(String traceId,long timeoutSecond) {
    return new Timer(traceId,timeoutSecond);
  }

  public static Timer newTimer(long timeoutSecond, long startTimestamp) {
    return new Timer(timeoutSecond,startTimestamp);
  }

  public static Timer newTimer(String traceId,long timeoutSecond, long startTimestamp) {
    return new Timer(traceId,timeoutSecond,startTimestamp);
  }

  public Timer newChildTimer() {
    return new Timer(traceId);
  }

  /**
   * 获取一个子 Timer，并保存
   * @param childName
   * @return
   */
  public Timer newChildTimerAndHold(String childName) {
    Timer timer = new Timer(traceId);
    if(!childMap.containsKey(childName)){
      childNameList.add(childName);
    }
    childMap.put(childName,timer);
    return timer;
  }

  public Timer newChildTimerWithSuffix(String suffix) {
    return new Timer(traceId+"-"+suffix);
  }

  /**
   * 获取一个子 Timer，并持有它
   * @param suffix
   * @param childName
   * @return
   */
  public Timer newChildTimerWithSuffixAndHold(String suffix,String childName) {
    Timer timer = new Timer(traceId+"-"+suffix);
    if(!childMap.containsKey(childName)){
      childNameList.add(childName);
    }
    childMap.put(childName,timer);
    return timer;
  }

  public Timer newChildTimerWithTimeout() {
    return new Timer(traceId,timeoutMillisecond);
  }

  /**
   * 获取一个子 Timer，并持有它
   * @param childName
   * @return
   */
  public Timer newChildTimerWithTimeoutAndHold(String childName) {
    Timer timer = new Timer(traceId,timeoutMillisecond);
    if(!childMap.containsKey(childName)){
      childNameList.add(childName);
    }
    childMap.put(childName,timer);
    return timer;
  }

  public Timer newChildTimerWithTimeoutAndSuffix(String suffix) {
    return new Timer(traceId+"-"+suffix,timeoutMillisecond);
  }

  /**
   * 获取一个子 Timer，并持有它
   * @param suffix
   * @param childName
   * @return
   */
  public Timer newChildTimerWithTimeoutAndSuffixAndHold(String suffix,String childName) {
    Timer timer = new Timer(traceId+"-"+suffix,timeoutMillisecond);
    if(!childMap.containsKey(childName)){
      childNameList.add(childName);
    }
    childMap.put(childName,timer);
    return timer;
  }

  public Timer newChildTimerWithAll() {
    return new Timer(traceId,timeoutMillisecond,startTimestamp);
  }

  /**
   * 获取一个子 Timer，并持有它
   * @param childName
   * @return
   */
  public Timer newChildTimerWithAllAndHold(String childName) {
    Timer timer = new Timer(traceId,timeoutMillisecond,startTimestamp);
    if(!childMap.containsKey(childName)){
      childNameList.add(childName);
    }
    childMap.put(childName,timer);
    return timer;
  }

  public Timer newChildTimerWithAllAndSuffix(String suffix) {
    return new Timer(traceId+"-"+suffix,timeoutMillisecond,startTimestamp);
  }

  /**
   * 获取一个子 Timer，并持有它
   * @param suffix
   * @param childName
   * @return
   */
  public Timer newChildTimerWithAllAndSuffixAndHold(String suffix,String childName) {
    Timer timer = new Timer(traceId+"-"+suffix,timeoutMillisecond,startTimestamp);
    if(!childMap.containsKey(childName)){
      childNameList.add(childName);
    }
    childMap.put(childName,timer);
    return timer;
  }

  public Timer getChild(String childName){
    return childMap.get(childName);
  }

  public Collection<Timer> getChildren(){
    return childMap.values();
  }

  public Set<String> getChildrenName(){
    return childMap.keySet();
  }

  /**
   * 创建一个用于计时的Timer
   */
  public Timer() {
    reset();
  }

  /**
   * 创建一个用于计时的Timer
   */
  public Timer(String traceId) {
    this.traceId = traceId;
    reset();
  }

  /**
   * 创建一个用于判断超时的Timer
   */
  public Timer(long timeoutSecond) {
    reset();
    this.timeoutMillisecond = timeoutSecond * 1000;
  }

  /**
   * 创建一个用于判断超时的Timer
   */
  public Timer(String traceId, long timeoutSecond) {
    this.traceId = traceId;
    reset();
    this.timeoutMillisecond = timeoutSecond * 1000;
  }

  public Timer(long timeoutSecond, long startTimestamp) {
    reset();
    this.startTimestamp = startTimestamp;
    this.timeoutMillisecond = timeoutSecond * 1000;
  }

  public Timer(String traceId, long timeoutSecond, long startTimestamp) {
    this.traceId = traceId;
    reset();
    this.startTimestamp = startTimestamp;
    this.timeoutMillisecond = timeoutSecond * 1000;
  }

  public void reset() {
    this.endTimestamp = null;
    stepTimeConsumeList.clear();
    lastStepTimeConsumeList.clear();
    curStepStackTraceList.clear();
    stepTimeConsumeDescList.clear();
    childMap.clear();
    this.startTimestamp = System.currentTimeMillis();
  }

  public String getTraceId() {
    return traceId;
  }

  public void stop() {
    endTimestamp = System.currentTimeMillis();
  }

  public boolean isTimeout() {
    return elapsedMillisecond() > timeoutMillisecond;
  }

  public static void sleepSec(int sec) {
    sleepMillisecond(sec * 1000L);
  }

  public static void sleepMillisecond(long millisecond) {
    try {
      Thread.sleep(millisecond);
    } catch (InterruptedException e) {
      log.info("Timer has been interrupted, e:", e);
      Thread.currentThread().interrupt();
    }
  }

  public static void sleepWithCondition(long sec, AtomicBoolean condition, boolean runCondition) {
    while (sec-- > 0 && condition.get() == runCondition) {
      sleepSec(1);
    }
  }

  public static void sleepMillSecWithCondition(long millSec, AtomicBoolean condition,
                                               boolean runCondition) {
    long sleepSec = millSec / 1000;
    sleepWithCondition(sleepSec, condition, runCondition);
    long remainMillSec = millSec % 1000;
    if (remainMillSec != 0 && condition.get() == runCondition) {
      sleepMillisecond(remainMillSec);
    }
  }

  public long getStartTimestamp() {
    return startTimestamp;
  }

  @Override
  public String toString() {
    return "Timer[" + elapsedMillisecond() + "ms]";
  }

  public long elapsedSecond() {
    return elapsedMillisecond() / 1000;
  }

  public long elapsedMillisecond() {
    if (endTimestamp == null) {
      return System.currentTimeMillis() - startTimestamp;
    } else {
      return endTimestamp - startTimestamp;
    }
  }

  public long stepElapsedMillisecond() {
    return stepElapsedMillisecond(null);
  }

  public long stepElapsedMillisecond(String stepDesc) {
    long lastStepTime = System.currentTimeMillis();
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    int curStackTraceIndex = 0;
    for (int i = stackTrace.length-1; i >=0; i--) {
      StackTraceElement stackTraceElement = stackTrace[i];
      String className = stackTraceElement.getClassName();
      String methodName = stackTraceElement.getMethodName();
      if(this.getClass().getName().equals(className) && "stepElapsedMillisecond".equals(methodName)){
        curStackTraceIndex = i;
        break;
      }
    }
    // 获取非当前类的当前方法栈的前一个
    StackTraceElement stackTraceElement = stackTrace[(curStackTraceIndex + 1)>=stackTrace.length ? curStackTraceIndex : (curStackTraceIndex + 1)];
    String className = stackTraceElement.getClassName();
    String methodName = stackTraceElement.getMethodName();
    int lineNumber = stackTraceElement.getLineNumber();
    String fileName = stackTraceElement.getFileName();
    String stackTraceElementStr = String.format("%s.%s(%s:%d)", className, methodName, fileName,lineNumber);
    if (stepTimeConsumeList.isEmpty()) {
      long step = lastStepTime - startTimestamp;
      lastStepTimeConsumeList.add(startTimestamp);
      lastStepTimeConsumeList.add(lastStepTime);
      stepTimeConsumeList.add(step);
      curStepStackTraceList.add(stackTraceElementStr);
      stepTimeConsumeDescList.add((stepDesc == null || "".equals(stepDesc.trim()))?String.format("step: [ %s ] cost",(stepTimeConsumeList.size())):String.format("step: [ %s ] cost",stepDesc));
    } else {
      long lastStep = lastStepTimeConsumeList.get(lastStepTimeConsumeList.size() - 1);
      long step = lastStepTime - lastStep;
      stepTimeConsumeList.add(step);
      lastStepTimeConsumeList.add(lastStepTime);
      curStepStackTraceList.add(stackTraceElementStr);
      stepTimeConsumeDescList.add((stepDesc == null || "".equals(stepDesc.trim()))?String.format("step: [ %s ] cost",(stepTimeConsumeList.size())):String.format("step: [ %s ] cost",stepDesc));
    }
    return stepTimeConsumeList.get(stepTimeConsumeList.size()-1);
  }

  /**
   * 智能展示时间单位 如：2d20m30s
   ==================================================[ traceId：9fa1ed4f-1d53-4e26-9de1-565691a9ff59 ]================================================== print all step start :
   step: [ aa123456789123456789009876543212ss ] cost :           1 s 14 ms   , step by : cdp.tag.util.Timer#main 33 line in ( Timer.java )
   step: [ bb ] cost :            2 s 6 ms   , step by : cdp.tag.util.Timer#main 36 line in ( Timer.java )
   step: [ 3 ] cost :                0 ms   , step by : cdp.tag.util.Timer#main 37 line in ( Timer.java )
   step: [ cc ] cost :           3 s 13 ms   , step by : cdp.tag.util.Timer#main 39 line in ( Timer.java )
   step: [ 5 ] cost :                0 ms   , step by : cdp.tag.util.Timer#main 40 line in ( Timer.java )
   total cost :           6 s 33 ms
   */
  public void printAllStepIntelligentShowUnit(){
    printAllStepIntelligentShowUnit(true,true);
  }

  /**
   * 智能展示时间单位 如：2d20m30s
   */
  public void printAllStepIntelligentShowUnit(boolean showStepStack,boolean printChildren){
    StringBuilder sb = new StringBuilder();
    sb.append("==================================================[ traceId：" + traceId + " ]==================================================" + " print all step start :");
    sb.append("\n");
    long total = 0;
    for (int i = 0; i < stepTimeConsumeDescList.size(); i++) {
      String desc = stepTimeConsumeDescList.get(i);
      sb.append(String.format("%96s : %20s , until cur step total cost: %20s ", desc,format(stepTimeConsumeList.get(i)),format(lastStepTimeConsumeList.get(i)-startTimestamp+stepTimeConsumeList.get(i))));
      total += stepTimeConsumeList.get(i);
      if(showStepStack){
        sb.append(String.format(" , step by : %s ", curStepStackTraceList.get(i)));
      }
      sb.append("\n");
    }
    if(stepTimeConsumeDescList.isEmpty()){
      sb.append(String.format("%96s","not has record step or is reset"));
    }else{
      sb.append(String.format("%96s : %20s ", "total cost",format(total)));
    }
    if(printChildren){
      String innerPrintPrefix = "";
      for (int i = 0; i < 1; i++) {
        innerPrintPrefix += "\t";
      }
      for (String childName : childNameList) {
        Timer value = childMap.get(childName);
        sb.append("\n");
        sb.append(String.format("%schild : [ %s ]\n", innerPrintPrefix,childName));
        sb.append(value.getAllStepIntelligentShowUnit(showStepStack, "\t",printChildren,2));
      }
    }
    log.info("\n"+sb.toString()+"\n");
  }

  /**
   * 智能展示时间单位 如：2d20m30s
   */
  public String getAllStepIntelligentShowUnit(){
    return getAllStepIntelligentShowUnit(true,true,1);
  }

  /**
   * 智能展示时间单位 如：2d20m30s
   */
  public String getAllStepIntelligentShowUnit(boolean showStepStack,boolean printChildren){
    return getAllStepIntelligentShowUnit(showStepStack,null,printChildren,1);
  }

  /**
   * 智能展示时间单位 如：2d20m30s
   */
  public String getAllStepIntelligentShowUnit(boolean showStepStack,boolean printChildren,int parentLevel){
    return getAllStepIntelligentShowUnit(showStepStack,null,printChildren,parentLevel);
  }

  public String getAllStepIntelligentShowUnit(boolean showStepStack,String printPrefix,boolean printChildren,int parentLevel){
    StringBuilder sb = new StringBuilder();
    if(printPrefix == null){
      printPrefix = "";
    }
    sb.append(String.format("%s%96s : %s\n",printPrefix, "traceId",traceId));
    long total = 0;
    for (int i = 0; i < stepTimeConsumeDescList.size(); i++) {
      String desc = stepTimeConsumeDescList.get(i);
      sb.append(String.format("%s%96s : %20s , until cur step total cost: %20s ",printPrefix, desc,format(stepTimeConsumeList.get(i)),format(lastStepTimeConsumeList.get(i)-startTimestamp+stepTimeConsumeList.get(i))));
      total += stepTimeConsumeList.get(i);
      if(showStepStack){
        sb.append(String.format(" , step by : %s ", curStepStackTraceList.get(i)));
      }
      sb.append("\n");
    }
    sb.append(String.format("%s%96s : %20s ",printPrefix, "total cost",format(total)));
    if(printChildren){
      String innerPrintPrefix = "";
      for (int i = 0; i < parentLevel; i++) {
        innerPrintPrefix += "\t";
      }
      for (String childName : childNameList) {
        Timer value = childMap.get(childName);
        sb.append("\n");
        sb.append(String.format("%schild : [ %s ]\n", innerPrintPrefix,childName));
        sb.append(value.getAllStepIntelligentShowUnit(showStepStack, innerPrintPrefix,printChildren,++parentLevel));
      }
    }
    return sb.toString();
  }

  public String format(long betweenMs) {
    StringBuilder sb = new StringBuilder();
    if (betweenMs > 0L) {
      long day = betweenMs / DateUnit.DAY.getMillis();
      long hour = betweenMs / DateUnit.HOUR.getMillis() - day * 24L;
      long minute = betweenMs / DateUnit.MINUTE.getMillis() - day * 24L * 60L - hour * 60L;
      long BetweenOfSecond = ((day * 24L + hour) * 60L + minute) * 60L;
      long second = betweenMs / DateUnit.SECOND.getMillis() - BetweenOfSecond;
      long millisecond = betweenMs - (BetweenOfSecond + second) * 1000L;
      int level = this.level.ordinal();
      int levelCount = 0;
      if (this.isLevelCountValid(levelCount) && 0L != day && level <= Level.DAY.ordinal()) {
        sb.append(day).append(" ").append(Level.DAY.name).append(" ");
        ++levelCount;
      }

      if (this.isLevelCountValid(levelCount) && 0L != hour && level <= Level.HOUR.ordinal()) {
        sb.append(hour).append(" ").append(Level.HOUR.name).append(" ");
        ++levelCount;
      }

      if (this.isLevelCountValid(levelCount) && 0L != minute && level <= Level.MINUTE.ordinal()) {
        sb.append(minute).append(" ").append(Level.MINUTE.name).append(" ");
        ++levelCount;
      }

      if (this.isLevelCountValid(levelCount) && 0L != second && level <= Level.SECOND.ordinal()) {
        sb.append(second).append(" ").append(Level.SECOND.name).append(" ");
        ++levelCount;
      }

      if (this.isLevelCountValid(levelCount) && 0L != millisecond && level <= Level.MILLISECOND.ordinal()) {
        sb.append(millisecond).append(" ").append(Level.MILLISECOND.name).append(" ");
      }
    }

    if (sb.toString() == null || "".equals(sb.toString().trim())) {
      sb.append(betweenMs).append(" ").append(Level.MILLISECOND.name).append(" ");
    }

    return sb.toString();
  }

  private boolean isLevelCountValid(int levelCount) {
    return this.levelMaxCount <= 0 || levelCount < this.levelMaxCount;
  }

  public static enum Level {
    DAY("d"),
    HOUR("h"),
    MINUTE("m"),
    SECOND("s"),
    MILLISECOND("ms");

    private final String name;

    private Level(String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }
  }

  public enum DateUnit {
    MS(1L),
    SECOND(1000L),
    MINUTE(SECOND.getMillis() * 60L),
    HOUR(MINUTE.getMillis() * 60L),
    DAY(HOUR.getMillis() * 24L),
    WEEK(DAY.getMillis() * 7L);

    private final long millis;

    private DateUnit(long millis) {
      this.millis = millis;
    }

    public long getMillis() {
      return this.millis;
    }
  }


}