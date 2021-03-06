package com.wu.flink.function;

import com.wu.flink.dto.WordWithCountDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * description:TODO
 *
 * @author simpson
 * @create 2021/04/15
 **/
@Slf4j
public class MyKeyOperatorStateFunction extends RichFlatMapFunction<WordWithCountDTO, WordWithCountDTO> implements CheckpointedFunction {
    private ValueState<Integer> currentState;
    private transient ListState<WordWithCountDTO> checkPointCountList;
    private List<WordWithCountDTO> listBufferElements = new ArrayList<WordWithCountDTO>();

    @Override
    public void flatMap(WordWithCountDTO wordWithCountDTO, Collector<WordWithCountDTO> collector) throws Exception {
        Integer value = currentState.value();
        log.info("MyKeyOperatorStateFunction.flatMap,currentState-->value={}", value);
        if (value == null) {
            value = 0;
        }
        value = value + wordWithCountDTO.count;
        currentState.update(value);

        WordWithCountDTO resultDTO = new WordWithCountDTO(wordWithCountDTO.word, wordWithCountDTO.count, value, wordWithCountDTO.timestamp);
        listBufferElements.add(resultDTO);

        collector.collect(resultDTO);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.info("MyKeyOperatorStateFunction.snapshotState,context={}", context);
        //???????????????clear,????????????????????????????????????????????????checkpoint????????????
        checkPointCountList.clear();
        for (int i = 0; i < listBufferElements.size(); i++) {
            checkPointCountList.add(listBufferElements.get(i));
        }

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        log.info("MyKeyOperatorStateFunction.initializeState,context={}", context);
        //1.???ListState????????????????????????,??????????????????ListStateDescriptor???
        ListStateDescriptor<WordWithCountDTO> listStateDescriptor = new ListStateDescriptor<WordWithCountDTO>("listForThree", TypeInformation.of(new TypeHint<WordWithCountDTO>() {
        }));
        //2.???????????????,?????????????????????????????????????????????ListState
        checkPointCountList = context.getOperatorStateStore().getListState(listStateDescriptor);

        //3.??????????????????????????????
        if (context.isRestored()) {
            log.info("MyKeyOperatorStateFunction.initializeState,isRestored={}", context.isRestored());
            //??????????????????????????????
            for (WordWithCountDTO element : checkPointCountList.get()) {
                log.info("MyKeyOperatorStateFunction.initializeState,checkPointCountList.get={}", element);
                listBufferElements.add(element);
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("MyKeyOperatorStateFunction.open,parameters={}", parameters);
        currentState = getRuntimeContext().getState(new ValueStateDescriptor<>("state", Integer.class));
    }
}
