/*! ******************************************************************************
*
* Pentaho Data Integration
*
* Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/

package com.laozhang.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerStep extends BaseStep implements StepInterface {

  private static final Class<?> PKG = KafkaConsumerStepMeta.class; // for i18n purposes

  //构造器需将参数传给父类构造器
  public KafkaConsumerStep(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis ) {
    super( s, stepDataInterface, c, t, dis );
  }

  KafkaConsumer<String, String> consumer = null;
  /**
   * 步骤启动时被PDI调用
   * 初始化步骤需要的逻辑
   * 强制调用super.init() 保证正确的行为
   * 一般像建立数据库连接或文件句柄在这里执行
   */
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    // Casting to step-specific implementation classes is safe
    KafkaConsumerStepMeta meta = (KafkaConsumerStepMeta) smi;
    KafkaConsumerStepData data = (KafkaConsumerStepData) sdi;
    if ( !super.init( meta, data ) ) {
      return false;
    }
    // todo 增加你自己的初始化逻辑
    try {
      Properties props = new Properties();
      props.put("bootstrap.servers", meta.getBootstrapServers());
      props.put("group.id", meta.getGroupId());
      props.put("enable.auto.commit", true);
      props.put("auto.commit.interval.ms", 1000);
      props.put("max.poll.records", 1000);
      props.put("session.timeout.ms", 30000);
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      consumer = new KafkaConsumer<String, String>(props);

      String topic = meta.getTopic();
      String partition = meta.getPartition();

      List<TopicPartition> pList = null;
      if (partition != null && partition.contains(",")) {
        String[] partitions = partition.split(",");
        pList = new ArrayList<>();
        for(int i = 0,len = partitions.length; i < len; i++) {
          pList.add(new TopicPartition(topic, Integer.parseInt(partitions[i])));
        }
      }else{
        pList = Arrays.asList(new TopicPartition(topic, 0));
      }

      this.consumer.assign(pList);
      if (meta.isLasted()) {
        this.consumer.seekToEnd(pList);
      }
    }catch (Exception ex){
      logError("初始化kafka失败");
      return false;
    }
    return true;
  }

  /**
   * 一旦转换开始执行，processRow() 只要返回true就一直被PDI重复调用
   * 当调用setOutputDone() 并且返回false 时，转换停止
   * 通过getRow() 获取输入的单行数据，然后改变或增加行内容，调用putRow()把变化的数据传递下一个步骤。
   * 如果getRow()返回null，说明没有更多的数据传入，调用 setOutputDone() 并且返回 false ，说明步骤不再处理。
   *
   * 使用RowDataUtil.allocateRowData(numberOfFields) 增加行内容并生成一个新的Object[]对象
   */
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    KafkaConsumerStepMeta meta = (KafkaConsumerStepMeta) smi;
    KafkaConsumerStepData data = (KafkaConsumerStepData) sdi;

    // "first" 标志执行第一次调用，可用于一次性获取一些行结构信息
    if ( first ) {
      first = false;
      data.outputRowMeta = new RowMeta();
      meta.getFields(data.outputRowMeta, getStepname(), null, null, this, null, null);
    }
    ConsumerRecords<String, String> records = consumer.poll(10000);
    Object[] outputRow;
    for (ConsumerRecord<String, String> record : records) {
      outputRow = new Object[1];
      outputRow[0] = record.value();
      putRow(data.outputRowMeta, outputRow);
    }
    logBasic( BaseMessages.getString( PKG, "consume.batch.info", records.count() ) ); // Some basic logging
    return true;
  }

  /**
   * 步骤执行结束以后调用
   * 作用跟init（）相反，常用于释放资源，如数据库连接
   * 强制调用 super.dispose() 保证正确的行为
   */
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    KafkaConsumerStepMeta meta = (KafkaConsumerStepMeta) smi;
    KafkaConsumerStepData data = (KafkaConsumerStepData) sdi;

  // todo 增加你自己的销毁、回收逻辑
    if(consumer != null){
      consumer.close();
    }
    super.dispose( meta, data );
  }
}
