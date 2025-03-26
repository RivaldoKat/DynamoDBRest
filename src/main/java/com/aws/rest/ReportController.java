package com.aws.rest;

import jxl.write.WriteException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("api/items:report")
public class ReportController {

    private final DynamoDBService dynamoDBService;
    private final WriteExcel writeExcel;
    private final WriteExcel.SendMessages sm;

    @Autowired()
    ReportController(DynamoDBService dynamoDBService, WriteExcel writeExcel, WriteExcel.SendMessages sm){
        this.dynamoDBService = dynamoDBService;
        this.writeExcel = writeExcel;
        this.sm = sm;

    }

    @PostMapping("")
    public String sendReport(@RequestBody Map<String, String> body) {
        var list = dynamoDBService.getOpenItems();

        try{
            InputStream is = writeExcel.write(list);
            sm.sendReport(is, body.get("email"));
            return "Report generated & sent";
        } catch(IOException | WriteException e) {
            e.printStackTrace();
        }
        return "Failed to generate report";
    }
}
