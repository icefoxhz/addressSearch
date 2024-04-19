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
    
            // =============== fir 算分， 每找到一个得5分
            int every_score_fir = 5;
            double found_count = 0.0;
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
            double fir_score = found_count * every_score_fir;
            all_found_count += found_count;
            all_value_count += query_value_length;
    
            // =================== mid 算分
            // mid占20分
            int MID_ALL_SCORE = 20;
            double mid_score = 0.0;
            found_count = 0.0;
            query_value_length = params.query_value_mid.length;
            query_field_length = params.query_fields_mid.length;
            if (query_value_length == 1) {
                String mid_val = params.query_value_mid[0];
                if (doc[params.query_field_building_number].value == Integer.parseInt(mid_val)) {
                    mid_score =  MID_ALL_SCORE;
                    found_count += 1;
                }
            }
            // 如果有多于1个值
            if (query_value_length > 1) {
                int avg_score = MID_ALL_SCORE / query_value_length;
                for (int i = 0; i < query_value_length; i++) {
                    if (doc.containsKey(params.query_fields_mid[i]) && doc[params.query_fields_mid[i]].size() > 0) {
                         if (doc[params.query_fields_mid[i]].value == params.query_value_mid[i]) {
                            found_count += 1;
                         }
                    }
                }
                mid_score =  avg_score * found_count;
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
                return score + mid_score_de + last_score_de;
            }
    
            score = base_score + fir_score + mid_score + last_score;
            if (score >= 100){
                score = 100 * (all_value_count / all_found_count);
            }
    
            return score;
        """
        }
    }
