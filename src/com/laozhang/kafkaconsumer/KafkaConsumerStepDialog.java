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

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.LabelText;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class KafkaConsumerStepDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = KafkaConsumerStepMeta.class; // 用于国际化

  // 保存了步骤的配置,当对话框打开时读配置,当对话框确认时写配置
  private KafkaConsumerStepMeta meta;

  // todo start 添加你的控件属性
  private LabelText wKfkBootstrapServers;
  private LabelText wTopic;
  private LabelText wPartition;
  private LabelText wConsumerGroupId;
  private Button bIsLasted;
  // todo end

  /**
   * 构造器需要显式调用super()来保存输入的meta对象，便于步骤随后的读写
   */
  public KafkaConsumerStepDialog(Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    meta = (KafkaConsumerStepMeta) in;
  }

  /**
   * 当用户打开步骤配置对话框是由Spoon客户端调用。
   * 当用户关闭对话框时，open()至少执行一次
   *
   *  若用户在对话框中点了确认，meta对象（构造时传入）必须更新至与新配置相符。
   *  changed 标识反映对话框的是否发生变更，需跟实际保持一致
   *
   *  若用户取消对话框，meta对象不能被更新，changed 标识保持原样。
   *
   *  确认时，返回步骤名
   *  取消时，返回null
   */
  public String open() {
    // 顺便保存SWT变量
    Shell parent = getParent();
    Display display = parent.getDisplay();

    // 准备对话框的SWT 代码
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, meta );

    // 保存changed标志到meta对象，如果用户取消按这个值恢复
    // changed 属性变量从 BaseStepDialog 类继承中获得
    changed = meta.hasChanged();

    // ModifyListener 用于监听所有控件，当发生操作时，更新meta对象changed标志
    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        meta.setChanged();
      }
    };

    // ------------------------------------------------------- //
    // 用于构建配置对话框的SWT 代码                            //
    // ------------------------------------------------------- //
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "Demo.Shell.Title" ) );
    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // 步骤名文本标签
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );

    // 步骤名文本框，用于设置步骤名
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    // todo start 添加你的控件
    // 业务文本标签 + 文本框

    wKfkBootstrapServers = new LabelText( shell, BaseMessages.getString( PKG, "kafka.bootstrap.servers.Label" ), null );
    props.setLook(wKfkBootstrapServers);
    wKfkBootstrapServers.addModifyListener( lsMod );
    wKfkBootstrapServers.setLayoutData( getFormDataFromPre(wStepname, margin));


    wTopic = new LabelText( shell, BaseMessages.getString( PKG, "kafka.topic.Label" ), null );
    props.setLook(wTopic);
    wTopic.addModifyListener( lsMod );
    wTopic.setLayoutData( getFormDataFromPre(wKfkBootstrapServers, margin));


    wPartition = new LabelText( shell, BaseMessages.getString( PKG, "kafka.partition.Label" ), null );
    props.setLook(wPartition);
    wPartition.addModifyListener( lsMod );
    wPartition.setLayoutData( getFormDataFromPre(wTopic, margin));


    wConsumerGroupId = new LabelText( shell, BaseMessages.getString( PKG, "kafka.group.id.Label" ), null );
    props.setLook(wConsumerGroupId);
    wConsumerGroupId.addModifyListener( lsMod );
    wConsumerGroupId.setLayoutData( getFormDataFromPre(wPartition, margin));

    bIsLasted = new Button(shell,SWT.CHECK);
    bIsLasted.setSelection(true);
    bIsLasted.setText(BaseMessages.getString( PKG, "kafka.lasted.Button" ));
    bIsLasted.setLayoutData(getFormDataFromPre(wConsumerGroupId, margin));
    // todo end

    // OK、 cancel 按钮
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    setButtonPositions( new Button[] { wOK, wCancel }, margin, bIsLasted );

    // OK、 cancel 按钮的事件监听器
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    // 默认监听器，响应回车
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wStepname.addSelectionListener( lsDef );
    wKfkBootstrapServers.addSelectionListener( lsDef );
    wTopic.addSelectionListener( lsDef );
    wPartition.addSelectionListener( lsDef );
    wConsumerGroupId.addSelectionListener( lsDef );
    bIsLasted.addSelectionListener( lsDef );

    // 点X 或 ALT-F4 或其他可以关掉、取消窗口的操作
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // 基于屏幕上的位置 设置或恢复对话框大小
    // 该方法从 BaseStepDialog 类继承中获得
    setSize();

    // meta对象默认值设置给对话框
    populateDialog();

    // 恢复到初始值
    meta.setChanged( changed );

    // 打开对话框，进去事件循环
    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    // ok 或cancel被执行时，调用dispose，此时对话框跳出事件循环，马上关闭。
    // stepname变量从BaseStepDialog类继承获得
    return stepname;
  }

  private FormData getFormDataFromPre(Control preControl,int topMargin){
    FormData bsValName = new FormData();
    bsValName.left = new FormAttachment( 0, 0 );
    bsValName.right = new FormAttachment( 100, 0 );
    bsValName.top = new FormAttachment( preControl, topMargin );
    return bsValName;
  }

  /**
   * 设置元数据对象中的默认值 给 对话框相关控件
   */
  private void populateDialog() {
    if(meta.getBootstrapServers() == null || meta.getGroupId() == null){
      meta.setDefault();
    }
    wStepname.selectAll();
    // todo start 使用meta对象中的默认值初始化你的控件信息
    wKfkBootstrapServers.setText( meta.getBootstrapServers());
    wTopic.setText( meta.getTopic());
    wPartition.setText( meta.getPartition());
    wConsumerGroupId.setText( meta.getGroupId());
    bIsLasted.setSelection( meta.isLasted());
    // todo start
  }

  //取消时调用
  private void cancel() {
    // stepname变量由open()方法返回
    // 取消时必须返回null
    stepname = null;
    // 恢复元数据对象（实现StepMetaInterface接口） changed标志到原来的样子
    meta.setChanged( changed );
    // 释放关掉对话框
    dispose();
  }

  //确认时调用
  private void ok() {
    // stepname变量由open()方法返回
    // 确认时必须返回步骤名。步骤名可能会改，所以返回步骤名称输入框里的内容即可。
    stepname = wStepname.getText();
    // 使用getter setter方法更新元数据对象（实现StepMetaInterface接口）的数据
    // todo start 使用控件编辑的数据设置到meta对象中
    meta.setBootstrapServers(wKfkBootstrapServers.getText());
    meta.setTopic(wTopic.getText());
    meta.setPartition(wPartition.getText());
    meta.setGroupId(wConsumerGroupId.getText());
    meta.setLasted(bIsLasted.getSelection());
    // todo end
    // 释放关掉对话框
    dispose();
  }
}
