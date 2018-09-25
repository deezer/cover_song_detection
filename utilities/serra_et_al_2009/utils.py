import os
import glob


def rename_txts(path):
	files = glob.glob(os.path.join(path, '*'))
	for f in files:
		if f.endswith('.txt'):
			new_f = f.split("_")[5]+'_'+f
			os.system("mv %s %s" %(f, new_f))
	return

def format_col_text_for_docker(path):
	files = os.listdir(path)
        for txt_file in files:
                if txt_file.endswith(".txt"):
                        fname = txt_file
                        f = open(path+txt_file)
                        data = f.readlines()
                        new_txt = [line.replace("data","mnt") for line in data]
			print new_txt
                        f.close()
                        savelist_to_file(new_txt, path+fname)
        return


def format_newline_col(path):
        files = glob.glob(os.path.join(path, '*'))
        for txt_file in files:
                if txt_file.endswith('.txt'):
                        fname = txt_file
                        f = open(txt_file)
                        data = f.readlines()
                        new_txt = [x for x in data if x!='\n']
                        f.close()
                        savelist_to_file(new_txt, fname)
        return


def savelist_to_file(pathList, filename):
        """
        """
        doc = open(filename,'w')
        for item in pathList:
                doc.write("%s" % item)
        doc.close()
        return
