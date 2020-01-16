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

package com.laozhang.patitioner.hash;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.PartitionerPlugin;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.BasePartitioner;
import org.pentaho.di.trans.Partitioner;
import org.w3c.dom.Node;

@PartitionerPlugin (
    id = "HashPartitioner",
    name = "Hash Partitioner",
    description = "Laozhang's Partition by Hash Code of String field"
  )
public class HashPartitioner extends BasePartitioner implements Partitioner {

  private static final Class<?> PKG = HashPartitionerDialog.class;

  private String fieldName;

  protected int partitionColumnIndex = -1;

  public HashPartitioner() {
    super();
  }

  public Partitioner getInstance() {
    Partitioner partitioner = new HashPartitioner();
    partitioner.setId( getId() );
    partitioner.setDescription( getDescription() );
    return partitioner;
  }

  public HashPartitioner clone() {
    HashPartitioner hashPartitioner = (HashPartitioner) super.clone();
    hashPartitioner.fieldName = fieldName;

    return hashPartitioner;
  }

  public String getDialogClassName() {
    return HashPartitionerDialog.class.getName();
  }

  public int getPartition( RowMetaInterface rowMeta, Object[] row ) throws KettleException {

    // 第一步应该调用init(),父类BasePartitioner中的属性才能被初始化
    init( rowMeta );

    // 确定分区号
    if ( partitionColumnIndex < 0 ) {
      partitionColumnIndex = rowMeta.indexOfValue( fieldName );
      if ( partitionColumnIndex < 0 ) {
        throw new KettleStepException( "Unable to find partitioning field name [" + fieldName + "] in the output row..." + rowMeta );
      }
    }

    // 获取被分区的字段值
    String partitionColumnValue = rowMeta.getString( row, partitionColumnIndex );
    int hash = partitionColumnValue.hashCode();
    //根据字段值的hash code 计算出归属分区号
    return (hash & 0x7FFFFFFF) % nrPartitions;
  }

  public String getDescription() {
    String description = "Laozhang's Partition by Hash Code of String field";
    if ( !Const.isEmpty( fieldName ) ) {
      description += " (" + fieldName + ")";
    }
    return description;
  }

  public String getXML() {
    StringBuilder xml = new StringBuilder();
    xml.append( "           " ).append( XMLHandler.addTagValue( "field_name", fieldName ) );
    return xml.toString();
  }

  public void loadXML( Node partitioningMethodNode ) throws KettleXMLException {
    fieldName = XMLHandler.getTagValue( partitioningMethodNode, "field_name" );
  }

  public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, "PARTITIONING_FIELDNAME", fieldName );
  }

  public void loadRep( Repository rep, ObjectId id_step ) throws KettleException {
    fieldName = rep.getStepAttributeString( id_step, "PARTITIONING_FIELDNAME" );
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }
}
