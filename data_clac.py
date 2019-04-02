# coding: utf-8

import json


def main():
    # 数据文件路径
    filename = './livestreamdb34.txt'
    output = './outputlivestreamdb34.txt'
    output_error = './errortlivestreamdb34.txt'
    # 定义列索引号
    column_map = {
        'id': 0,
        'service_type': 1,
        'job_type': 2,
        'expect_start_time': 4,
        'start_time': 8
    }

    # 统计结果字典
    count_dict = {}

    with open(filename,encoding="utf-8") as f:
        # 按行读取文件
        lines = f.readlines()
        for line in lines:
            # 按制表符分隔行
            segs = line.split('\t')
            # 从行中提取service_type, job_type字段
            service = segs[column_map['service_type']].strip('"')
            job = segs[column_map['job_type']].strip('"')

            # 初始化统计字典
            if service not in count_dict.keys():
                count_dict[service] = {job: {'total': 0, 'count': 0,'error':0, 'delay_lines': []}}
            else:
                if job not in count_dict[service].keys():
                    count_dict[service][job] = {'total': 0, 'count': 0,'error':0, 'delay_lines': []}

            count_dict[service][job]['total'] += 1

            try:
                # 从行中提取except_start_time和start_time字段，有可能因数据报错，要做异常处理
                expect_start_time = int(segs[column_map['expect_start_time']].strip('"\n'))
                start_time = int(segs[column_map['start_time']].strip('"\n'))
            except Exception as e:
                # 记录错误数
                count_dict[service][job]['error'] += 1
                with open(output_error, 'a',encoding='utf-8') as o:
                    o.write(line)
                # 打印错误行id，exception
                # print(segs[column_map['service_type']],segs[column_map['job_type']],segs[column_map['id']], str(e))
                # 跳过此行
                continue

            # 计数
            if start_time - expect_start_time > 10000:
                count_dict[service][job]['count'] += 1
                count_dict[service][job]['delay_lines'].append(line)

    # 格式化统计结果
    for k, v in count_dict.items():
        for kk, vv in v.items():
            vv['rate'] = '%.2f' % (vv['count'] / vv['total'] * 100) + '%'
            # 写文件
            with open(output, 'a', encoding='utf-8') as o:
                o.writelines(vv['delay_lines'])
            del vv['delay_lines']

    print(json.dumps(count_dict, indent=4))


if __name__ == '__main__':
    main()
