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

import java.util.List;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step(
  id = "KafkaConsumerStep",
  name = "DemoStep.Name",
  description = "DemoStep.TooltipDesc",
  image = "com/laozhang/kafkaconsumer/resources/demo.svg",
  categoryDescription = "Common.Category",
  i18nPackageName = "com.laozhang.kafkaconsumer",
  documentationUrl = "DemoStep.DocumentationURL",
  casesUrl = "DemoStep.CasesURL",
  forumUrl = "DemoStep.ForumURL"
  )
@InjectionSupported( localizationPrefix = "KafkaConsumerStepMeta.Injection." )
public class KafkaConsumerStepMeta extends BaseStepMeta implements StepMetaInterface {

  private static final Class<?> PKG = KafkaConsumerStepMeta.class;

  private String bootstrapServers;
  private String topic;
  private String partition;
  private String groupId;
  private boolean lasted;

  public KafkaConsumerStepMeta() {
    super();
  }

  public StepDialogInterface getDialog( Shell shell, StepMetaInterface meta, TransMeta transMeta, String name ) {
    return new KafkaConsumerStepDialog(shell,meta,transMeta,name);
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans disp ) {
    return new KafkaConsumerStep( stepMeta, stepDataInterface, cnr, transMeta, disp );
  }

  public StepDataInterface getStepData() {
    return new KafkaConsumerStepData();
  }

  /**
   * 每一次新步骤被创建，需重设步骤配置默认值
   */
  public void setDefault() {
    setBootstrapServers("192.168.1.2:9200");
    setGroupId("kafkaConsumerDemo");
    setLasted(true);
    setPartition("all");
    setTopic("state");
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getPartition() {
    return partition;
  }

  public void setPartition(String partition) {
    this.partition = partition;
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public boolean isLasted() {
    return lasted;
  }

  public void setLasted(boolean lasted) {
    this.lasted = lasted;
  }

  /** todo
   * 步骤在Spoon中创建多个时，需要返回一个步骤meta对象的深拷贝。
   * 若有属性是引用类型对象，需重新实现深拷贝
   */
  public Object clone() {
    KafkaConsumerStepMeta retval = (KafkaConsumerStepMeta)super.clone();
    return retval;
  }

  /** todo
   * 这个方法由Spoon调用，将配置序列化为xml
   * 返回值为xml一个或多个标签的文本片段
   * 可使用org.pentaho.di.core.xml.XMLHandler来生成xml
   */
  public String getXML() throws KettleValueException {
    StringBuilder xml = new StringBuilder();
    // only one field to serialize
    xml.append( XMLHandler.addTagValue( "bootstrapServers", bootstrapServers ) );
    xml.append( XMLHandler.addTagValue( "topic", topic ) );
    xml.append( XMLHandler.addTagValue( "partition", partition ) );
    xml.append( XMLHandler.addTagValue( "groupId", groupId ) );
    xml.append( XMLHandler.addTagValue( "lasted", lasted ) );
    return xml.toString();
  }

  /** todo
   * 当步骤需要从xml加载配置时，由PDI调用
   * 同样推荐使用 org.pentaho.di.core.xml.XMLHandler 来读取

   * @param stepnode  xml格式的配置数据
   * @param databases  转换中可用的数据库元数据
   * @param metaStore 可选的元数据
   */
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    try {
      setBootstrapServers( XMLHandler.getNodeValue( XMLHandler.getSubNode( stepnode, "bootstrapServers" ) ) );
      setTopic( XMLHandler.getNodeValue( XMLHandler.getSubNode( stepnode, "topic" ) ) );
      setPartition( XMLHandler.getNodeValue( XMLHandler.getSubNode( stepnode, "partition" ) ) );
      setGroupId( XMLHandler.getNodeValue( XMLHandler.getSubNode( stepnode, "groupId" ) ) );
      setLasted(Boolean.parseBoolean(XMLHandler.getNodeValue( XMLHandler.getSubNode( stepnode, "lasted" ) ) ) );
    } catch ( Exception e ) {
      throw new KettleXMLException( "plugin unable to read step info from XML node", e );
    }
  }

  /**
   * 当把配置序列化到资源库时，由Spoon调用
   *
   * @param rep                 保存的目标资源库名
   * @param metaStore           保存的meta shore
   * @param id_transformation   转换id
   * @param id_step             步骤id
   */
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
      throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "bootstrapServers", bootstrapServers );
      rep.saveStepAttribute( id_transformation, id_step, "topic", topic );
      rep.saveStepAttribute( id_transformation, id_step, "partition", partition );
      rep.saveStepAttribute( id_transformation, id_step, "groupId", groupId );
      rep.saveStepAttribute( id_transformation, id_step, "lasted", lasted );
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step into repository: " + id_step, e );
    }
  }

  /**
   * 把配置从资源库读出，由Spoon调用
   * 
   * @param rep        目标资源库名
   * @param metaStore  目标meta shore
   * @param id_step    步骤id
   * @param databases  转换中可用的数据库元数据
   */
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
      throws KettleException {
    try {
      bootstrapServers  = rep.getStepAttributeString( id_step, "bootstrapServers" );
      topic  = rep.getStepAttributeString( id_step, "topic" );
      partition  = rep.getStepAttributeString( id_step, "partition" );
      groupId  = rep.getStepAttributeString( id_step, "groupId" );
      lasted  = rep.getStepAttributeBoolean( id_step, "lasted" );
    } catch ( Exception e ) {
      throw new KettleException( "Unable to load step from repository", e );
    }
  }

  /**
   * 该方法用于反映步骤对行流做出的改变，即反映步骤输出的行结构
   * 
   * @param inputRowMeta    步骤输入的行流结构对象the row structure coming in to the step
   * @param name         变更名
   * @param info        步骤输入的行流结构信息
   * @param nextStep      下一个步骤元数据
   * @param space        变量空间
   * @param repository    资源库实例
   * @param metaStore      metaStore
   */
  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
    inputRowMeta.addValueMeta( getValueMetaString(name,"message"));
  }

  private ValueMetaInterface getValueMetaString(String name,String fieldName){
    ValueMetaString v = new ValueMetaString(fieldName);
    v.setTrimType( ValueMetaInterface.TRIM_TYPE_BOTH );
    v.setOrigin(name);
    return v;
  }
  /**
   * This method is called when the user selects the "Verify Transformation" option in Spoon. 
   * A list of remarks is passed in that this method should add to. Each remark is a comment, warning, error, or ok.
   * The method should perform as many checks as necessary to catch design-time errors.
   * 
   * Typical checks include:
   * - verify that all mandatory configuration is given
   * - verify that the step receives any input, unless it's a row generating step
   * - verify that the step does not receive any input if it does not take them into account
   * - verify that the step finds fields it relies on in the row-stream
   * 
   *   @param remarks    the list of remarks to append to
   *   @param transMeta  the description of the transformation
   *   @param stepMeta  the description of the step
   *   @param prev      the structure of the incoming row-stream
   *   @param input      names of steps sending input to the step
   *   @param output    names of steps this step is sending output to
   *   @param info      fields coming in from info steps 
   *   @param metaStore  metaStore to optionally read from
   */
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
      IMetaStore metaStore ) {
    CheckResult cr;

    // See if there are input streams leading to this step!
    if ( input != null && input.length > 0 ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK,
        BaseMessages.getString( PKG, "Demo.CheckResult.ReceivingRows.OK" ), stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR,
        BaseMessages.getString( PKG, "Demo.CheckResult.ReceivingRows.ERROR" ), stepMeta );
      remarks.add( cr );
    }
  }
}
