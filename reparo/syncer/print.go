// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package syncer

import (
	"fmt"
	"strings"
	"unicode"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
)

type printSyncer struct{
	flashback bool	
}

var _ Syncer = &printSyncer{}

func newPrintSyncer(flashback bool) (*printSyncer, error) {
	p := new(printSyncer)
	p.flashback = flashback
	return p, nil
}

func (p *printSyncer) Sync(pbBinlog *pb.Binlog, cb func(binlog *pb.Binlog)) error {
	switch pbBinlog.Tp {
	case pb.BinlogType_DDL:
		printDDL(pbBinlog)
		cb(pbBinlog)
	case pb.BinlogType_DML:
		for _, event := range pbBinlog.GetDmlData().GetEvents() {
                        if p.flashback {
                        	printFlashBack(&event)    
                        } else {
				printEvent(&event)
			}
		}
		cb(pbBinlog)
	default:
		return errors.Errorf("unknown type: %v", pbBinlog.Tp)

	}

	return nil
}

func (p *printSyncer) Close() error {
	return nil
}


func printEvent(event *pb.Event) {
	printHeader(event)

	switch event.GetTp() {
	case pb.EventType_Insert:
		err := printInsertOrDeleteEvent(event.Row)
		if err != nil {
			log.Error("print insert event failed", zap.Error(err))
		}
	case pb.EventType_Update:
		err := printUpdateEvent(event.Row)
		if err != nil {
			log.Error("print update event failed", zap.Error(err))
		}
	case pb.EventType_Delete:
		err := printInsertOrDeleteEvent(event.Row)
		if err != nil {
			log.Error("print delete event failed", zap.Error(err))
		}
	}
}


func printHeader(event *pb.Event) {
	printEventHeader(event)
}

func printDDL(binlog *pb.Binlog) {
	fmt.Printf("##DDL query: %s\n", binlog.DdlQuery)
}



func printEventHeader(event *pb.Event) {
	fmt.Printf("##schema: %s; table: %s; type: %s\n", event.GetSchemaName(), event.GetTableName(), event.GetTp())
}


func printFlashBack(event *pb.Event) {
        switch event.GetTp() {
        case pb.EventType_Insert:
		err := printFlashBackInsertEvent(event)
		if err != nil {
			log.Error("flashback insert event failed", zap.Error(err))
		}
        case pb.EventType_Update:
		err := printFlashBackUpdateEvent(event)
		if err != nil {
			log.Error("flashback update event failed", zap.Error(err))
		}
        case pb.EventType_Delete:
		err := printFlashBackDeleteEvent(event)
		if err != nil {
			log.Error("print delete event failed", zap.Error(err))
		}
        }

}

func getValueString(datatype string,value string) string{
   var result string 
   switch datatype{
     case  "text", "longtext", "mediumtext", "char", "tinytext", "varchar", "var_string","date", "datetime", "time", "timestamp", "year", "varbinary":
	if value != "<nil>" {
		value = strings.Replace(value, "\\", "\\\\", -1)
		value = strings.Replace(value, "'", "\\'", -1)
        	result= "'"+value+"'"		
	} else {
		result = "NULL"
	}
     case "bit":
	if value != "<nil>" {
                result= "b'"+value+"'"
        } else {
                result = "NULL"
        }
     case "binary":
	if value !=  "<nil>" {
		clean := strings.Map(func(r rune) rune {
    			if unicode.IsPrint(r) {
        			return r
    			}
    			return -1
		}, value)	
		result = "binary'" +clean + "'"
        } else {
                result = "NULL"
        }
     default:
	result=value         
}
	return result
}

func printFlashBackUpdateEvent(event *pb.Event) error {
	var i int
        i=0
	fmt.Printf("UPDATE %s.%s SET ", event.GetSchemaName(), event.GetTableName())
	for _, c := range event.Row {
		i++
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return errors.Annotate(err, "unmarshal failed")
		}

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return errors.Annotate(err, "decode row failed")
		}


		tp := col.Tp[0]
		if i<len(event.Row){
			fmt.Printf(" %s = %s , ", col.Name, getValueString(col.MysqlType ,formatValueToString(val, tp)))
		} else{

			fmt.Printf(" %s = %s ", col.Name, getValueString(col.MysqlType ,formatValueToString(val, tp)))
		}
	}
	fmt.Printf(" WHERE ")
	for _, c := range event.Row {
                col := &pb.Column{}
                err := col.Unmarshal(c)
                if err != nil {
                        return errors.Annotate(err, "unmarshal failed")
                }


                _, changedVal, err := codec.DecodeOne(col.ChangedValue)
                if err != nil {
                        return errors.Annotate(err, "decode row failed")
                }

                tp := col.Tp[0]
		if getValueString(col.MysqlType,formatValueToString(changedVal, tp)) == "NULL"{
			fmt.Printf(" %s IS NULL AND ", col.Name)
		}else{
                	fmt.Printf(" %s =%s AND ", col.Name, getValueString(col.MysqlType,formatValueToString(changedVal, tp)))
		}
        }
        fmt.Printf(" 1=1; \n ")
	return nil
}

func printUpdateEvent(row [][]byte) error {
	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return errors.Annotate(err, "unmarshal failed")
		}

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return errors.Annotate(err, "decode row failed")
		}

		_, changedVal, err := codec.DecodeOne(col.ChangedValue)
		if err != nil {
			return errors.Annotate(err, "decode row failed")
		}

		tp := col.Tp[0]
		fmt.Printf("%s(%s): %s => %s\n", col.Name, col.MysqlType, formatValueToString(val, tp), formatValueToString(changedVal, tp))
	}
	return nil
}




func printFlashBackDeleteEvent(event *pb.Event) error {
	fmt.Printf("INSERT INTO  %s.%s VALUES(  ", event.GetSchemaName(), event.GetTableName())
        var insql strings.Builder
	//insql.WriteString("INSERT INTO  %s.%s VALUES(  ", event.GetSchemaName(), event.GetTableName())
	for _, c := range event.Row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return errors.Annotate(err, "unmarshal failed")
		}

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return errors.Annotate(err, "decode row failed")
		}

		tp := col.Tp[0]
		//fmt.Printf(" %s, ",getValueString(col.MysqlType,formatValueToString(val, tp)))
		insql.WriteString(getValueString(col.MysqlType,formatValueToString(val, tp))+",")
	}
	rc := insql.String()
	rc = strings.TrimRight(rc, ",")
	fmt.Printf("%s ); \n", rc)
	return nil
}



func printFlashBackInsertEvent(event *pb.Event) error {
	fmt.Printf("DELETE FROM %s.%s WHERE ", event.GetSchemaName(), event.GetTableName())
        for _, c := range event.Row {
                col := &pb.Column{}
                err := col.Unmarshal(c)
                if err != nil {
                        return errors.Annotate(err, "unmarshal failed")
                }   

                _, val, err := codec.DecodeOne(col.Value)
                if err != nil {
                        return errors.Annotate(err, "decode row failed")
                }   

                tp := col.Tp[0]
		if getValueString(col.MysqlType,formatValueToString(val, tp)) == "NULL"{
                        fmt.Printf(" %s IS NULL AND ", col.Name)
                }else{  
                        fmt.Printf(" %s =%s AND ", col.Name, getValueString(col.MysqlType,formatValueToString(val, tp)))
                }
        }   
        fmt.Printf(" 1=1; \n ")
        return nil 
}

func printInsertOrDeleteEvent(row [][]byte) error {
	for _, c := range row {
		col := &pb.Column{}
		err := col.Unmarshal(c)
		if err != nil {
			return errors.Annotate(err, "unmarshal failed")
		}

		_, val, err := codec.DecodeOne(col.Value)
		if err != nil {
			return errors.Annotate(err, "decode row failed")
		}

		tp := col.Tp[0]
		fmt.Printf("%s(%s): %s\n", col.Name, col.MysqlType, formatValueToString(val, tp))
	}
	return nil
}


