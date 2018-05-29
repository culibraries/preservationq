import unittest
import  os,requests,json

class testInstallRequirements(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory
        self.test_file = os.path.join(os.getenv('ETDTGT','/data/libetd-preservation/'), 'test.txt')
        self.api_token = os.getenv('APITOKEN',None)
        self.base_url = os.getenv('APIBASE',"https://geo.colorado.edu/api")

    def tearDown(self):
        try:
            # Remove the directory after the test
            os.remove(self.test_file)
        except:
            pass

    def test_path_exist_ETDSRC(self):
        self.assertTrue(os.path.exists(os.getenv('ETDSRC','/data/libetd-archive/')))
    def test_path_exist_ETDTGT(self):
        self.assertTrue(os.path.exists(os.getenv('ETDTGT','/data/libetd-preservation/')))
    def test_permissions_write_ETDTGT(self):
        test_dir = os.getenv('ETDTGT','/data/libetd-preservation/')
        # Create a file in the temporary directory
        with open(self.test_file, 'w') as f:
            # Write something to it
            f.write('This test sucks the hind tit')
        # Reopen the file and check if what we read back is the same
        with open(self.test_file,'r') as f:
            self.assertEqual(f.read(), 'This test sucks the hind tit')
    def test_valid_apitoken(self):
        user_url = "{0}/user/?format=json".format(self.base_url)
        headers={'Content-Type':'application/json','Authorization':'Token {0}'.format(self.api_token)}
        req = requests.get(user_url,headers=headers)
        try:
            try:
                data=req.json()
                self.assertEqual(self.api_token,data["auth-token"])
            except:
                raise
        except:
            self.assertEqual(self.api_token,req.text)

if __name__ == '__main__':
    unittest.main()
