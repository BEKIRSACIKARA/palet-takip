from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_cors import CORS
import hashlib
import jwt
import datetime
import os
import openpyxl
from openpyxl.styles import Font, PatternFill
from io import BytesIO
from functools import wraps
import psycopg2
import psycopg2.extras
import urllib.parse
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import zipfile
import csv

app = Flask(__name__, static_folder='static')
app.config['SECRET_KEY'] = 'palet-takip-gizli-anahtar-2026'
CORS(app)

PALET_TIPLERI = [("P001", "Euro Palet"), ("P002", "Sanayi Paleti"), ("P003", "Plastik Palet")]
SAHIP_TIP_DEPO = "DEPO"
SAHIP_TIP_DAGITICI = "DAGITICI"
SAHIP_TIP_MUSTERI = "MUSTERI"

def hash_sifre(sifre):
    return hashlib.sha256(sifre.encode()).hexdigest()

def get_db_connection():
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        urllib.parse.uses_netloc.append('postgres')
        url = urllib.parse.urlparse(database_url)
        return psycopg2.connect(
            database=url.path[1:], user=url.username,
            password=url.password, host=url.hostname, port=url.port
        )
    else:
        import sqlite3
        conn = sqlite3.connect('palet_takip.db')
        conn.row_factory = sqlite3.Row
        return conn

def veritabani_olustur():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS kullanicilar (id SERIAL PRIMARY KEY, kullanici_adi TEXT UNIQUE NOT NULL, sifre TEXT NOT NULL, tip TEXT NOT NULL, ad_soyad TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS musteriler (id SERIAL PRIMARY KEY, musteri_kodu TEXT UNIQUE NOT NULL, musteri_adi TEXT NOT NULL, tabela_adi TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS palet_tipleri (id SERIAL PRIMARY KEY, stok_kodu TEXT UNIQUE NOT NULL, palet_adi TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS stoklar (id SERIAL PRIMARY KEY, stok_sahibi_tip TEXT NOT NULL, stok_sahibi_id INTEGER NOT NULL, palet_tipi_id INTEGER NOT NULL, miktar INTEGER DEFAULT 0, FOREIGN KEY (palet_tipi_id) REFERENCES palet_tipleri(id), UNIQUE(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS hareketler (id SERIAL PRIMARY KEY, tarih TEXT NOT NULL, yapan_kullanici_id INTEGER NOT NULL, hareket_tipi TEXT NOT NULL, gonderen_tip TEXT NOT NULL, gonderen_id INTEGER NOT NULL, alan_tip TEXT NOT NULL, alan_id INTEGER NOT NULL, palet_tipi_id INTEGER NOT NULL, miktar INTEGER NOT NULL, aciklama TEXT, makbuz_no TEXT, FOREIGN KEY (yapan_kullanici_id) REFERENCES kullanicilar(id), FOREIGN KEY (palet_tipi_id) REFERENCES palet_tipleri(id))''')
    
    try:
        cursor.execute("ALTER TABLE hareketler ADD COLUMN makbuz_no TEXT")
        conn.commit()
    except:
        conn.rollback()

    cursor.execute('''CREATE TABLE IF NOT EXISTS makbuzlar (id SERIAL PRIMARY KEY, makbuz_no TEXT UNIQUE NOT NULL, tarih TEXT NOT NULL, islem_tipi TEXT NOT NULL, gonderen_tip TEXT NOT NULL, gonderen_id INTEGER NOT NULL, gonderen_adi TEXT NOT NULL, alan_tip TEXT NOT NULL, alan_id INTEGER NOT NULL, alan_adi TEXT NOT NULL, toplam_miktar INTEGER NOT NULL, aciklama TEXT, yapan_kullanici_id INTEGER NOT NULL, yapan_kullanici_adi TEXT NOT NULL, FOREIGN KEY (yapan_kullanici_id) REFERENCES kullanicilar(id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS makbuz_detaylari (id SERIAL PRIMARY KEY, makbuz_id INTEGER NOT NULL, palet_tipi_id INTEGER NOT NULL, stok_kodu TEXT NOT NULL, palet_adi TEXT NOT NULL, miktar INTEGER NOT NULL, FOREIGN KEY (makbuz_id) REFERENCES makbuzlar(id), FOREIGN KEY (palet_tipi_id) REFERENCES palet_tipleri(id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS ayarlar (key TEXT PRIMARY KEY, value TEXT NOT NULL)''')
    conn.commit()
    
    for stok_kodu, palet_adi in PALET_TIPLERI:
        cursor.execute("INSERT INTO palet_tipleri (stok_kodu, palet_adi) SELECT %s, %s WHERE NOT EXISTS (SELECT 1 FROM palet_tipleri WHERE stok_kodu = %s)", (stok_kodu, palet_adi, stok_kodu))
    
    cursor.execute("INSERT INTO kullanicilar (kullanici_adi, sifre, tip, ad_soyad) SELECT %s, %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM kullanicilar WHERE kullanici_adi = %s)", ('depocu', hash_sifre('1234'), 'DEPOCU', 'Ana Depocu', 'depocu'))
    conn.commit()
    cursor.close()
    conn.close()

def stok_miktari_getir(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT miktar FROM stoklar WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s AND palet_tipi_id = %s", (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id))
    sonuc = cursor.fetchone()
    cursor.close()
    conn.close()
    return sonuc[0] if sonuc else 0

def stok_guncelle(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, degisim):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, miktar FROM stoklar WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s AND palet_tipi_id = %s", (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id))
    mevcut = cursor.fetchone()
    if mevcut:
        yeni_miktar = mevcut[1] + degisim
        if yeni_miktar < 0:
            cursor.close()
            conn.close()
            return False, "Stok yetersiz!"
        cursor.execute("UPDATE stoklar SET miktar = %s WHERE id = %s", (yeni_miktar, mevcut[0]))
    else:
        if degisim < 0:
            cursor.close()
            conn.close()
            return False, "Stok kaydı bulunamadı!"
        cursor.execute("INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar) VALUES (%s, %s, %s, %s)", (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, degisim))
    conn.commit()
    cursor.close()
    conn.close()
    return True, ""

def hareket_kaydet(yapan_kullanici_id, hareket_tipi, gonderen_tip, gonderen_id, alan_tip, alan_id, palet_tipi_id, miktar, aciklama="", makbuz_no=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO hareketler (tarih, yapan_kullanici_id, hareket_tipi, gonderen_tip, gonderen_id, alan_tip, alan_id, palet_tipi_id, miktar, aciklama, makbuz_no) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                   (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), yapan_kullanici_id, hareket_tipi, gonderen_tip, gonderen_id, alan_tip, alan_id, palet_tipi_id, miktar, aciklama, makbuz_no))
    conn.commit()
    cursor.close()
    conn.close()

def makbuz_no_olustur():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT MAX(id) FROM makbuzlar")
        son_id = cursor.fetchone()[0]
        yeni_no = (son_id + 1) if son_id else 1
    except:
        yeni_no = 1
    finally:
        cursor.close()
        conn.close()
    return f"PLT{str(yeni_no).zfill(5)}"

def makbuz_kaydet(transfer_data):
    conn = get_db_connection()
    cursor = conn.cursor()
    makbuz_no = makbuz_no_olustur()
    tarih = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    cursor.execute("INSERT INTO makbuzlar (makbuz_no, tarih, islem_tipi, gonderen_tip, gonderen_id, gonderen_adi, alan_tip, alan_id, alan_adi, toplam_miktar, aciklama, yapan_kullanici_id, yapan_kullanici_adi) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                   (makbuz_no, tarih, transfer_data['islem_tipi'], transfer_data['gonderen_tip'], transfer_data['gonderen_id'], transfer_data['gonderen_adi'], transfer_data['alan_tip'], transfer_data['alan_id'], transfer_data['alan_adi'], transfer_data['toplam_miktar'], transfer_data['aciklama'], transfer_data['yapan_id'], transfer_data['yapan_adi']))
    makbuz_id = cursor.fetchone()[0]
    for detay in transfer_data['detaylar']:
        cursor.execute("INSERT INTO makbuz_detaylari (makbuz_id, palet_tipi_id, stok_kodu, palet_adi, miktar) VALUES (%s, %s, %s, %s, %s)", (makbuz_id, detay['palet_tipi_id'], detay['stok_kodu'], detay['palet_adi'], detay['miktar']))
    conn.commit()
    cursor.close()
    conn.close()
    return makbuz_no

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'hata': 'Token gerekli'}), 401
        try:
            token = token.replace('Bearer ', '')
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            current_user = data
        except:
            return jsonify({'hata': 'Geçersiz token'}), 401
        return f(current_user, *args, **kwargs)
    return decorated

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, kullanici_adi, tip, ad_soyad FROM kullanicilar WHERE kullanici_adi = %s AND sifre = %s", (data.get('kullanici_adi'), hash_sifre(data.get('sifre'))))
    kullanici = cursor.fetchone()
    cursor.close()
    conn.close()
    if kullanici:
        token = jwt.encode({'id': kullanici[0], 'kullanici_adi': kullanici[1], 'tip': kullanici[2], 'ad_soyad': kullanici[3], 'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)}, app.config['SECRET_KEY'], algorithm='HS256')
        return jsonify({'success': True, 'token': token, 'kullanici': {'id': kullanici[0], 'kullanici_adi': kullanici[1], 'tip': kullanici[2], 'ad_soyad': kullanici[3]}})
    return jsonify({'success': False, 'hata': 'Hatalı kullanıcı adı veya şifre'}), 401

@app.route('/api/dagitici_listesi', methods=['GET'])
@token_required
def get_dagitici_listesi(current_user):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, kullanici_adi, ad_soyad FROM kullanicilar WHERE tip = 'DAGITICI' ORDER BY ad_soyad")
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([{'id': d[0], 'kullanici_adi': d[1], 'ad_soyad': d[2]} for d in sonuc])

@app.route('/api/tum_musteriler', methods=['GET'])
@token_required
def get_tum_musteriler(current_user):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, musteri_kodu, musteri_adi, tabela_adi FROM musteriler ORDER BY musteri_adi")
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([{'id': m[0], 'musteri_kodu': m[1], 'musteri_adi': m[2], 'tabela_adi': m[3]} for m in sonuc])

@app.route('/api/palet_tipleri', methods=['GET'])
@token_required
def get_palet_tipleri(current_user):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, stok_kodu, palet_adi FROM palet_tipleri")
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([{'id': p[0], 'stok_kodu': p[1], 'palet_adi': p[2]} for p in sonuc])

@app.route('/api/stok', methods=['GET'])
@token_required
def get_stok(current_user):
    tip, kimlik = request.args.get('tip'), request.args.get('id', type=int)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT pt.id, pt.stok_kodu, pt.palet_adi, COALESCE(s.miktar, 0) FROM palet_tipleri pt LEFT JOIN stoklar s ON pt.id = s.palet_tipi_id AND s.stok_sahibi_tip = %s AND s.stok_sahibi_id = %s ORDER BY pt.id", (tip, kimlik))
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([{'palet_id': p[0], 'stok_kodu': p[1], 'palet_adi': p[2], 'miktar': p[3]} for p in sonuc])

@app.route('/api/musteri_stoklari', methods=['GET'])
@token_required
def get_musteri_stoklari(current_user):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT m.id, m.musteri_kodu, m.musteri_adi, m.tabela_adi, pt.stok_kodu, pt.palet_adi, s.miktar 
                      FROM stoklar s JOIN musteriler m ON s.stok_sahibi_id = m.id 
                      JOIN palet_tipleri pt ON s.palet_tipi_id = pt.id 
                      WHERE s.stok_sahibi_tip = 'MUSTERI' AND s.miktar > 0 ORDER BY m.musteri_adi''')
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    musteriler = {}
    for row in sonuc:
        if row[0] not in musteriler:
            musteriler[row[0]] = {'id': row[0], 'musteri_kodu': row[1], 'musteri_adi': row[2], 'tabela_adi': row[3], 'stoklar': []}
        musteriler[row[0]]['stoklar'].append({'stok_kodu': row[4], 'palet_adi': row[5], 'miktar': row[6]})
    return jsonify(list(musteriler.values()))

@app.route('/api/transfer', methods=['POST'])
@token_required
def transfer_yap(current_user):
    data = request.get_json()
    hareket_tipi = data.get('hareket_tipi')
    palet_tipi_id = data.get('palet_tipi_id')
    miktar = data.get('miktar')
    alici_id = data.get('alici_id')

    if not hareket_tipi or not palet_tipi_id or not miktar:
        return jsonify({'hata': 'Eksik parametreler'}), 400
    if miktar <= 0:
        return jsonify({'hata': 'Miktar pozitif olmalı'}), 400

    kullanici_id, kullanici_tip, kullanici_adi, kullanici_kadi = current_user['id'], current_user['tip'], current_user['ad_soyad'], current_user['kullanici_adi']
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, stok_kodu, palet_adi FROM palet_tipleri WHERE id = %s", (palet_tipi_id,))
    palet = cursor.fetchone()
    
    if not palet:
        cursor.close()
        conn.close()
        return jsonify({'hata': 'Geçersiz palet tipi'}), 400

    transfer_data = {'islem_tipi': hareket_tipi, 'yapan_id': kullanici_id, 'yapan_adi': f"{kullanici_adi} ({kullanici_kadi})", 'detaylar': [], 'toplam_miktar': miktar}

    if kullanici_tip == 'DEPOCU':
        if hareket_tipi == 'DEPO_DAGITICI':
            gonderen_tip, gonderen_id, gonderen_adi = SAHIP_TIP_DEPO, 0, "DEPO"
            alan_tip, alan_id = SAHIP_TIP_DAGITICI, alici_id
            cursor.execute("SELECT ad_soyad FROM kullanicilar WHERE id = %s", (alici_id,))
            alan_adi = cursor.fetchone()[0]
            aciklama = f"Depodan {alan_adi} dağıtıcısına transfer"
        elif hareket_tipi == 'DAGITICI_DEPO':
            gonderen_tip, gonderen_id = SAHIP_TIP_DAGITICI, alici_id
            alan_tip, alan_id, alan_adi = SAHIP_TIP_DEPO, 0, "DEPO"
            cursor.execute("SELECT ad_soyad FROM kullanicilar WHERE id = %s", (alici_id,))
            gonderen_adi = cursor.fetchone()[0]
            aciklama = f"{gonderen_adi} dağıtıcısından depoya iade"
        elif hareket_tipi == 'DEPO_MUSTERI':
            gonderen_tip, gonderen_id, gonderen_adi = SAHIP_TIP_DEPO, 0, "DEPO"
            alan_tip, alan_id = SAHIP_TIP_MUSTERI, alici_id
            cursor.execute("SELECT musteri_adi FROM musteriler WHERE id = %s", (alici_id,))
            alan_adi = cursor.fetchone()[0]
            aciklama = f"Depodan {alan_adi} müşterisine direkt sevk"
        elif hareket_tipi == 'MUSTERI_DEPO':
            gonderen_tip, gonderen_id = SAHIP_TIP_MUSTERI, alici_id
            alan_tip, alan_id, alan_adi = SAHIP_TIP_DEPO, 0, "DEPO"
            cursor.execute("SELECT musteri_adi FROM musteriler WHERE id = %s", (alici_id,))
            gonderen_adi = cursor.fetchone()[0]
            aciklama = f"{gonderen_adi} müşterisinden depoya iade"
        else:
            return jsonify({'hata': 'Geçersiz işlem'}), 400
    elif kullanici_tip == 'DAGITICI':
        if hareket_tipi == 'DAGITICI_MUSTERI':
            gonderen_tip, gonderen_id, gonderen_adi = SAHIP_TIP_DAGITICI, kullanici_id, kullanici_adi
            alan_tip, alan_id = SAHIP_TIP_MUSTERI, alici_id
            cursor.execute("SELECT musteri_adi FROM musteriler WHERE id = %s", (alici_id,))
            alan_adi = cursor.fetchone()[0]
            aciklama = f"{alan_adi} müşterisine teslimat"
        elif hareket_tipi == 'MUSTERI_DAGITICI':
            gonderen_tip, gonderen_id = SAHIP_TIP_MUSTERI, alici_id
            alan_tip, alan_id, alan_adi = SAHIP_TIP_DAGITICI, kullanici_id, kullanici_adi
            cursor.execute("SELECT musteri_adi FROM musteriler WHERE id = %s", (alici_id,))
            gonderen_adi = cursor.fetchone()[0]
            aciklama = f"{gonderen_adi} müşterisinden toplama"
        else:
            return jsonify({'hata': 'Geçersiz işlem'}), 400

    transfer_data.update({'gonderen_tip': gonderen_tip, 'gonderen_id': gonderen_id, 'gonderen_adi': gonderen_adi, 'alan_tip': alan_tip, 'alan_id': alan_id, 'alan_adi': alan_adi, 'aciklama': aciklama})
    transfer_data['detaylar'].append({'palet_tipi_id': palet_tipi_id, 'stok_kodu': palet[1], 'palet_adi': palet[2], 'miktar': miktar})

    mevcut = stok_miktari_getir(gonderen_tip, gonderen_id, palet_tipi_id)
    if mevcut < miktar:
        return jsonify({'hata': f'Yetersiz stok! Mevcut: {mevcut}'}), 400

    stok_guncelle(gonderen_tip, gonderen_id, palet_tipi_id, -miktar)
    stok_guncelle(alan_tip, alan_id, palet_tipi_id, +miktar)
    
    makbuz_no = makbuz_kaydet(transfer_data)
    hareket_kaydet(kullanici_id, hareket_tipi, gonderen_tip, gonderen_id, alan_tip, alan_id, palet_tipi_id, miktar, aciklama, makbuz_no)
    
    cursor.close()
    conn.close()
    return jsonify({'success': True, 'mesaj': 'Transfer başarılı', 'makbuz_no': makbuz_no})

@app.route('/api/makbuz/<makbuz_no>', methods=['GET'])
@token_required
def makbuz_goster(current_user, makbuz_no):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM makbuzlar WHERE makbuz_no = %s", (makbuz_no,))
    makbuz = cursor.fetchone()
    if not makbuz:
        return jsonify({'hata': 'Makbuz yok'}), 404
    cursor.execute("SELECT stok_kodu, palet_adi, miktar FROM makbuz_detaylari WHERE makbuz_id = %s", (makbuz[0],))
    detaylar = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify({'makbuz_no': makbuz[1], 'tarih': makbuz[2], 'islem_tipi': makbuz[3], 'gonderen_adi': makbuz[6], 'alan_adi': makbuz[9], 'toplam_miktar': makbuz[10], 'aciklama': makbuz[11], 'yapan_adi': makbuz[13], 'detaylar': [{'stok_kodu': d[0], 'palet_adi': d[1], 'miktar': d[2]} for d in detaylar]})

# --- DASHBOARD VE DİĞER RAPORLAR (BURADA DEVAM EDİYOR...) ---
@app.route('/api/rapor/dashboard', methods=['POST'])
@token_required
def rapor_dashboard(current_user):
    data = request.get_json() or {}
    baslangic = f"{data.get('baslangic_tarihi', '2020-01-01')} 00:00:00"
    bitis = f"{data.get('bitis_tarihi', '2030-01-01')} 23:59:59"
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT SUM(miktar) FROM stoklar WHERE stok_sahibi_tip = 'DEPO'")
    depo = int(cursor.fetchone()[0] or 0)
    cursor.execute("SELECT SUM(miktar) FROM stoklar WHERE stok_sahibi_tip = 'DAGITICI'")
    dag = int(cursor.fetchone()[0] or 0)
    cursor.execute("SELECT SUM(miktar) FROM stoklar WHERE stok_sahibi_tip = 'MUSTERI'")
    mus = int(cursor.fetchone()[0] or 0)
    cursor.execute("SELECT SUM(miktar) FROM hareketler WHERE hareket_tipi = 'DAGITICI_MUSTERI' AND tarih BETWEEN %s AND %s", (baslangic, bitis))
    v = int(cursor.fetchone()[0] or 0)
    cursor.execute("SELECT SUM(miktar) FROM hareketler WHERE hareket_tipi = 'MUSTERI_DAGITICI' AND tarih BETWEEN %s AND %s", (baslangic, bitis))
    t = int(cursor.fetchone()[0] or 0)
    oran = round((t/v*100),1) if v > 0 else 0
    cursor.execute("SELECT m.musteri_adi, SUM(s.miktar) FROM stoklar s JOIN musteriler m ON s.stok_sahibi_id = m.id WHERE s.stok_sahibi_tip = 'MUSTERI' GROUP BY m.id ORDER BY 2 DESC LIMIT 5")
    bekleyen = [{'ad': r[0], 'miktar': r[1]} for r in cursor.fetchall()]
    cursor.execute("SELECT u.ad_soyad, SUM(CASE WHEN h.hareket_tipi='DAGITICI_MUSTERI' THEN miktar ELSE 0 END), SUM(CASE WHEN h.hareket_tipi='MUSTERI_DAGITICI' THEN miktar ELSE 0 END) FROM hareketler h JOIN kullanicilar u ON h.yapan_kullanici_id = u.id WHERE u.tip='DAGITICI' GROUP BY u.id ORDER BY 3 DESC")
    perf = [{'ad': r[0], 'verilen': r[1], 'toplanan': r[2]} for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify({'stok':{'depo':depo,'dagitici':dag,'musteri':mus}, 'verim':{'v':v,'t':t,'oran':oran}, 'bekleyen':bekleyen, 'perf':perf})

@app.route('/api/toplanacak_paletler', methods=['GET'])
@token_required
def toplanacak_paletler(current_user):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT m.id, m.musteri_kodu, m.musteri_adi, SUM(s.miktar) FROM stoklar s JOIN musteriler m ON s.stok_sahibi_id = m.id WHERE s.stok_sahibi_tip='MUSTERI' AND s.miktar > 0 GROUP BY m.id")
    sonuclar = []
    for m in cursor.fetchall():
        sonuclar.append({'musteri':f"{m[1]} - {m[2]}", 'toplam':m[3], 'g0_7':0, 'g8_14':0, 'g15_21':0, 'g22':m[3]})
    cursor.close()
    conn.close()
    return jsonify(sonuclar)

if __name__ == '__main__':
    veritabani_olustur()
    from waitress import serve
    serve(app, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
