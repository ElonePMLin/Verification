import math
import time
from collections import defaultdict
import chardet  # 检查csv文件编码
import pandas as pd
from multiprocessing import Process


class DataCheck:

    def __init__(self):
        self.mei_tuan_data = None
        self.mei_tuan_error = None
        self.tiktok_data = None
        self.tiktok_error = None
        self.like_data = None
        self.like_error = None

    @staticmethod
    def expected_data():
        print("券号 验券时间 套餐名 真实入账金额 真实平台")

    @classmethod
    def read_excel(cls, path, sheet_name):
        try:
            suffix = path.split(".")[-1]
            if suffix == "csv":
                encoded = "utf-8"
                with open(path, 'rb') as f:
                    encode = chardet.detect(f.read())
                    if "UTF-8" not in encode["encoding"]:
                        encoded = "GBK"
                    else:
                        encoded = "utf-8"
                return pd.read_csv(path, encoding=encoded)
            else:
                return pd.read_excel(path, sheet_name=sheet_name)
        except Exception as e:
            print("文件", e)
            return

    def mei_tuan(self, path, sheet_name):
        try:
            data = self.read_excel(path, sheet_name)
            data.rename(columns=data.iloc[1], inplace=True)
            data = data.iloc[2:]
            data.reset_index(inplace=True)
            # 美团的平台HO入账金额
            data['真实入账金额'] = data['总收入（元）'] + data['商家营销费用（元）']
            data = data[['券号', '验券时间', '套餐名', '真实入账金额']]
            data['真实入账金额'] = data.groupby(by="验券时间")['真实入账金额'].transform('sum').fillna(0).round(2)
            data['真实平台'] = "美团大众"
            data['券号'] = data['券号'].astype('string')
            # print(data.info())
            self.mei_tuan_data = data
            self.mei_tuan_error = None
        except Exception as e:
            print("美团", e)
            self.mei_tuan_error = "表格/工作表格式不符合要求"
            self.mei_tuan_data = None

    def tiktok(self, path, sheet_name):
        try:
            data = self.read_excel(path, sheet_name).rename(columns={
                "订单实收": "真实入账金额", "商品名称": "套餐名", "核销时间": "验券时间"
            })
            data["券码"] = data["券码"].astype("string")
            data["订单编号"] = data["订单编号"].astype("string")
            data1 = data[["券码", "验券时间", "套餐名", "真实入账金额"]].rename(columns={"券码": "券号"}).dropna()
            data2 = data[["订单编号", "验券时间", "套餐名", "真实入账金额"]].rename(columns={"订单编号": "券号"}).dropna()
            data = pd.concat([data1, data2])
            data["真实入账金额"] = data.groupby(by="券号")["真实入账金额"].transform("sum")
            data["券号"] = data["券号"].str.replace(".0", "")
            data["真实平台"] = "抖音"
            # print(data.info())
            self.tiktok_data = data
            self.tiktok_error = None
        except Exception as e:
            print("抖音", e)
            self.tiktok_data = None
            self.tiktok_error = "表格/工作表格式不符合要求"

    def like(self, path, sheet_name):
        try:
            data = self.read_excel(path, sheet_name)
            data = data[data["状态"] == "已核销"].rename(columns={
                "商品名称": "套餐名", "实付": "真实入账金额", "核销时间": "验券时间"
            })
            data["券码"] = data["券码"].astype("string")
            data["订单号"] = data["订单号"].astype("string")
            data1 = data[["券码", "验券时间", "套餐名", "真实入账金额"]].rename(columns={"券码": "券号"})
            data2 = data[["订单号", "验券时间", "套餐名", "真实入账金额"]].rename(columns={"订单号": "券号"})
            data = pd.concat([data1, data2])
            data["真实入账金额"] = data.groupby(by="券号")["真实入账金额"].transform("sum")
            data["真实平台"] = "有赞"
            # print(data.info())
            self.like_data = data
            self.like_error = None
        except Exception as e:
            print("有赞", e)
            self.like_data = None
            self.like_error = "表格/工作表格式不符合要求"


class WriteOffCheck:

    def __init__(self, path, sheet_name):
        self.original_data = DataCheck.read_excel(path, sheet_name)
        self.coupon2idx = defaultdict(list)
        self.convert2idx()
        self.data = None
        self.message = None
        self.done = False

    def convert2idx(self):
        try:
            self.original_data['平台HO入账金额'] = self.original_data['平台HO入账金额'].round(2)
            self.original_data['实际金额'] = 0
            self.original_data['备注'] = self.original_data['备注'].astype("string")
            self.original_data['验证券号1'] = self.original_data['验证券号1'].astype("string")
            self.original_data['验证券号2'] = self.original_data['验证券号2'].astype("string")
            self.original_data['验证券号3'] = self.original_data['验证券号3'].astype("string")
            for idx, coup1, coup2, coup3 in self.original_data.reset_index()[["index", "验证券号1", "验证券号2", "验证券号3"]].values:
                if not pd.isna(coup1):
                    for coup in coup1.split("、"):
                        self.coupon2idx[coup] = [idx, 1]
                if not pd.isna(coup2):
                    for coup in coup2.split("、"):
                        self.coupon2idx[coup] = [idx, 2]
                if not pd.isna(coup3):
                    for coup in coup2.split("、"):
                        self.coupon2idx[coup] = [idx, 3]
        except Exception as e:
            print("初始化：", e)
            self.message = "核销记录表不符合要求"

    def add_plat_remark(self, num, coup_merge):
        for idx, coupon, plat in coup_merge[coup_merge['平台'] != coup_merge['真实平台']][["index", f"验证券号{num}", "真实平台"]].values:
            remark = self.original_data.loc[idx, '备注']
            if pd.isna(remark):
                self.original_data.loc[idx, '备注'] = f"{coupon}渠道错误，正确应为{plat};"
            else:
                self.original_data.loc[idx, '备注'] += f"{coupon}渠道错误，正确应为{plat};"

    def update_coupon2idx(self, coups):
        for coup in coups:
            if coup in self.coupon2idx:
                self.coupon2idx.pop(coup)

    def update_balance(self, coups):
        for idx, balance in coups[["index", "真实入账金额"]].values:
            self.original_data.loc[idx, "实际金额"] += round(balance, 2)

    def first_check(self, data):
        try:
            write_off_data = self.original_data.reset_index()
            # 正确的coupon
            coupon_1 = write_off_data.merge(data, how="inner", left_on="验证券号1", right_on="券号")
            coupon_2 = write_off_data.merge(data, how="inner", left_on="验证券号2", right_on="券号")
            coupon_3 = write_off_data.merge(data, how="inner", left_on="验证券号3", right_on="券号")
            # 更新平台
            self.add_plat_remark(1, coupon_1)
            self.add_plat_remark(2, coupon_2)
            self.add_plat_remark(3, coupon_3)
            # 更新金额
            # coupon_1['实际金额'] = coupon_1['真实入账金额']
            # self.original_data.loc[coupon_1['index'], '实际金额'] = coupon_1['实际金额']
            self.update_balance(coupon_1)
            # coupon_2['实际金额'] = coupon_2['真实入账金额']
            # self.original_data.loc[coupon_2['index'], '实际金额'] = coupon_2['实际金额']
            self.update_balance(coupon_2)
            # coupon_3['实际金额'] = coupon_3['真实入账金额']
            # self.original_data.loc[coupon_3['index'], '实际金额'] = coupon_3['实际金额']
            self.update_balance(coupon_3)
            # 将检查过的coupon移除
            self.update_coupon2idx(coupon_1["验证券号1"].values)
            self.update_coupon2idx(coupon_2["验证券号2"].values)
            self.update_coupon2idx(coupon_3["验证券号3"].values)
        except Exception as e:
            print("第一次筛选：", e)
            self.message = "工作表数据格式不符合要求"

    def second_check(self, data):
        # 不在data的coupon，分4个进程进行处理
        coups2idx = list(self.coupon2idx.items())
        length = len(coups2idx)
        space = length // 5
        process = []
        for i in range(5):
            start = i * space
            end = (i + 1) * space
            if i == 4:
                end = length
            p = Process(
                target=self.second_check_process_start,
                args=(coups2idx[start: end], data)
            )
            process.append(p)
        for p in process:
            p.start()
            p.join()

    def second_check_process_start(self, coups, data):
        try:
            for coup, (idx, num) in coups:
                length1 = len(coup)
                cnt = math.floor(length1 * 0.2)
                for right, plat, balance in data[["券号", "真实平台", "真实入账金额"]].values:
                    length2 = len(right)
                    if abs(length1 - length2) <= cnt and self.levenshtein(coup, right) >= 0.8:
                        # 更新为正确的coupon（小错误）
                        self.original_data.loc[idx, f"验证券号{num}"] = right
                        # 更新平台
                        if self.original_data.loc[idx, "平台"] != plat:
                            remark = self.original_data.loc[idx, "备注"]
                            if pd.isna(remark):
                                self.original_data.loc[idx, "备注"] = f"{right}渠道错误，正确应为{plat};"
                            else:
                                self.original_data.loc[idx, "备注"] += f"{right}渠道错误，正确应为{plat};"
                        # 更新金额
                        self.original_data.loc[idx, "实际金额"] += round(balance, 2)
                        # 将检查过的coup移除
                        if coup in self.coupon2idx:
                            self.coupon2idx.pop(coup)
                        break
        except Exception as e:
            print("多进程中：", e)
            self.message = "工作表数据格式不符合要求"

    def final_check(self):
        try:
            write_off_data = self.original_data.reset_index()
            write_off_data = write_off_data[
                (write_off_data["平台"] == "美团大众") |
                (write_off_data["平台"] == "爱逛") |
                (write_off_data["平台"] == "有赞") |
                (write_off_data["平台"] == "抖音")
            ]
            for idx, remain, balance in write_off_data[~write_off_data["实际金额"].isna() & (write_off_data["实际金额"].round(2) != write_off_data["平台HO入账金额"].round(2)) & (write_off_data["实际金额"] != 0)][["index", "实际金额", "平台HO入账金额"]].values:
                remark = self.original_data.loc[idx, "备注"]
                if pd.isna(remark):
                    self.original_data.loc[idx, "备注"] = f"金额错误，正确为{round(remain, 2)};"
                else:
                    self.original_data.loc[idx, "备注"] += f"金额错误，正确为{round(remain, 2)};"
            write_off_data = self.original_data.reset_index()
            write_off_data = write_off_data[
                (write_off_data["平台"] != "美团大众") &
                # (self.original_data["平台"] != "爱逛") &
                (write_off_data["平台"] != "有赞") &
                (write_off_data["平台"] != "抖音")
            ]
            self.update_coupon2idx(write_off_data["验证券号1"].values)
            self.update_coupon2idx(write_off_data["验证券号2"].values)
            self.update_coupon2idx(write_off_data["验证券号3"].values)
            self.done = True
        except Exception as e:
            print("最后：", e)
            self.message = "工作表数据格式不符合要求"

    @classmethod
    def levenshtein(cls, a, b):
        # 都为空，则都相似
        if not a and not b:
            return 1
        # 一个为空，肯定不相似
        if not a or not b:
            return 0
        editDistance = cls.editDis(a, b)  # 计算编辑距离
        # print(editDistance)
        return 1 - (editDistance / max(len(a), len(b)))

    @classmethod
    def editDis(cls, a, b):
        alen = len(a)
        blen = len(b)
        if alen == 0:
            return alen
        if blen == 0:
            return blen
        # 二维数组
        v = [[0] * (blen + 1) for _ in range(alen + 1)]
        for i in range(alen + 1):
            for j in range(blen + 1):
                if i == 0:
                    v[i][j] = j
                elif j == 0:
                    v[i][j] = i
                elif a[i - 1] == b[j - 1]:
                    v[i][j] = v[i - 1][j - 1]
                else:
                    v[i][j] = 1 + min(v[i - 1][j - 1], min(v[i][j - 1], v[i - 1][j]))
        return v[alen][blen]


if __name__ == '__main__':
    start = time.time()
    expected = DataCheck()
    expected.mei_tuan("../res/24年3月/40121699_团购收支明细_20240301~20240331_17122901671941726357773206.xlsx", "订单收入明细表")
    expected.tiktok("../res/24年3月/核销记录_2024-03-01_2024-03-31.xlsx", "核销明细")
    expected.like("../res/24年3月/有赞核销3-4月.csv", "默认工作表")
    check = WriteOffCheck("../res/24年3月/2024.03月网络.xlsx", "Sheet1")
    print(len(check.coupon2idx))
    data = pd.concat([expected.mei_tuan_data, expected.tiktok_data, expected.like_data])
    # print(expected.tiktok_data[expected.tiktok_data["券号"] == "102156112454210"])
    # print(data[data["券号"] == "102156112454210"])
    check.first_check(data)
    check.second_check(data)
    check.final_check()
    # check.
    print(check.coupon2idx)
    print(len(check.coupon2idx))
    print(time.time() - start)
