"""
评分规则:
    找到主体 50 分
    找到楼栋 20分, 一共有2个位置，如果有2个值，则每个位置10分， 如果只有1个值，找到就得20分
    找到其他部分每个均 5分
    --------------
    全部分词都能匹配直接就是100分
    如果超过100分， 100 * (分词匹配到的个数 / 分词总数)
"""
SEARCH_SCORE_SCRIPT = {
    "script": {
        "lang": "painless",
        "source": """
            // 能进入这里的都是找到主体的，给个基础分
            double base_score = 50;
            double all_found_count = 0.0;  
            double all_value_count = 0.0;
            
            // ============= 区 (只参与减分)
            double de_region_score = 0.0;
            if (doc.containsKey(params.region_field) && doc[params.region_field].size() > 0){
                if (params.region_value != doc[params.region_field].value){
                    de_region_score = -5.0;
                }
            }
            
            // ==============街道 (只参与减分)
            double de_street_score = 0.0;
            if (doc.containsKey(params.street_field) && doc[params.street_field].size() > 0){
                if (params.street_value != doc[params.street_field].value){
                    de_street_score = -5.0;
                }
            }
            
            // =============== fir 算分， 每找到一个得5分
            double fir_score = 0.0;
            double found_count = 0.0;
            int every_score_fir = 5;
            int query_value_length = params.query_value_fir.length;
            int query_field_length = params.query_fields_fir.length;
            for (int i = 0; i < query_field_length; i++) {
                if (doc.containsKey(params.query_fields_fir[i]) && doc[params.query_fields_fir[i]].size() > 0) {
                    for (int j = 0; j < query_value_length; j++) {
                        if (doc[params.query_fields_fir[i]].value == params.query_value_fir[j]) {
                            found_count += 1;
                            break;
                        }
                    }
                }
            }
            fir_score = found_count * every_score_fir;
            
            // fir要算减分项
            double fir_score_de = 0.0;
            //if (fir_score > 0) {
                double containsFirKeyCount = 0.0;
                for (int i = 0; i < query_field_length; i++) {
                    if (doc.containsKey(params.query_fields_fir[i]) && doc[params.query_fields_fir[i]].size() > 0) {
                        containsFirKeyCount += 1.0;
                    }
                }
                
                if (query_value_length < containsFirKeyCount){
                    fir_score_de = (query_value_length - containsFirKeyCount) * every_score_fir;
                }
            //}
            // -------------------------
            
            all_found_count += found_count;
            all_value_count += query_value_length;
    
            // =================== mid 算分
            // mid占20分
            int MID_ALL_SCORE = 20;
            double mid_score = 0.0;
            found_count = 0.0;
            query_value_length = params.query_value_mid.length;
            query_field_length = params.query_fields_mid.length;
            // 第1个位置的值必须一一对应
            if (query_value_length == 1) {
                long building_number = params.query_value_building_number;
                if (doc[params.query_field_building_number].value == building_number) {
                    mid_score =  MID_ALL_SCORE;
                    found_count += 1;
                }
            }
            // 如果有多于1个值
            if (query_value_length > 1) {
                int avg_score = MID_ALL_SCORE / query_value_length;
                
                // 第1个位置mid_1的值必须对应
                if (doc.containsKey(params.query_fields_mid[0]) && doc[params.query_fields_mid[0]].size() > 0){
                    if (doc[params.query_fields_mid[0]].value == params.query_value_mid[0]) {
                        found_count += 1;
                    }
                }
                
                // 后面几个位置任意匹配
                for (int i = 1; i < query_field_length; i++) {
                    if (doc.containsKey(params.query_fields_mid[i]) && doc[params.query_fields_mid[i]].size() > 0) {
                        for (int j = 1; j < query_value_length; j++) {
                            if (doc[params.query_fields_mid[i]].value == params.query_value_mid[j]) {
                                found_count += 1;
                                break;
                            }
                        }
                    }
                }
                if (found_count == query_value_length){
                    mid_score = MID_ALL_SCORE;
                }else{
                    mid_score =  avg_score * found_count;
                }
            }
            
            // mid要算减分项
            double mid_score_de = 0.0;
            //if (mid_score > 0) {
                double containsMidKeyCount = 0.0;
                for (int i = 0; i < query_field_length; i++) {
                    if (doc.containsKey(params.query_fields_mid[i]) && doc[params.query_fields_mid[i]].size() > 0) {
                        containsMidKeyCount += 1.0;
                    }
                }
                
                if (query_value_length < containsMidKeyCount){
                    mid_score_de = (query_value_length - containsMidKeyCount) * (MID_ALL_SCORE / containsMidKeyCount);
                }
            //}
            // -------------------------
            
            all_found_count += found_count;
            all_value_count += query_value_length;
    
            // ================ last 算分，每找到一个得n分
            int every_score_last = 4;
            found_count = 0.0;
            query_value_length = params.query_value_last.length;
            query_field_length = params.query_fields_last.length;
            for (int i = 0; i < query_field_length; i++) {
                if (doc.containsKey(params.query_fields_last[i]) && doc[params.query_fields_last[i]].size() > 0) {
                    for (int j = 0; j < query_value_length; j++) {
                        if (doc[params.query_fields_last[i]].value == params.query_value_last[j]) {
                            found_count += 1;
                            break;
                        }
                    }
                }
            }
            double last_score = found_count * every_score_last;
            
            // last要算减分项
            double last_score_de = 0.0;
            //if (last_score > 0) {
                double containsLastKeyCount = 0.0;
                for (int i = 0; i < query_field_length; i++) {
                    if (doc.containsKey(params.query_fields_last[i]) && doc[params.query_fields_last[i]].size() > 0) {
                        containsLastKeyCount += 1.0;
                    }
                }
                if (query_value_length < containsLastKeyCount){
                    last_score_de = 0 - every_score_last; // 多了就 -n分
                }
            //}
            // -------------------------
            
            all_found_count += found_count;
            all_value_count += query_value_length;
    
            // 找到数量的百分比作为评分
            double score = 0.0;
            if (all_value_count == all_found_count){
                score = 100;
            }else{
                score = base_score + fir_score + mid_score + last_score;
                if (score >= 100){
                    score = 100;
                }
            }
            //return (int)all_found_count;
            
            int multi_region = params.multi_region;
            if (multi_region == 1){
                return (int)score + de_region_score + de_street_score + fir_score_de + mid_score_de + last_score_de;
            }
            return (int)score + fir_score_de + mid_score_de + last_score_de;
        """
        }
    }
